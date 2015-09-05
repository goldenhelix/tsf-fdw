/*-------------------------------------------------------------------------
 *
 * tsf_fdw.c
 *
 * Function definitions for TSF foreign data wrapper. These functions access
 * data stored in TSF through the official C driver.
 *
 * Credit:
 *
 * This FDW heavily leaned on the mongo_fdw
 * [https://github.com/citusdata/mongo_fdw] as an implementation
 * guide. Thanks to Citus Data for making their FDW open source.
 *
 * Also, the restriction parsing and evaluation is heavily modeled after
 * the Python FDW module Multicorn libary by Kozea
 * [https://github.com/Kozea/Multicorn/]
 *
 * Copyright (c) 2015 Golden Helix, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "tsf_fdw.h"

#include <sys/stat.h>

#include "tsf.h"
#include "stringbuilder.h"
#include "query.h"

#include "access/reloptions.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "commands/vacuum.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "nodes/makefuncs.h"
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "optimizer/plancat.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/var.h"
#include "parser/parsetree.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/hsearch.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/syscache.h"

#if PG_VERSION_NUM >= 90300
#include "access/htup_details.h"
#endif

#if SIZEOF_DATUM != 8
#error "Only 64-bit machines supported. sizeof(void*) should be 8"
#endif

#ifndef USE_FLOAT8_BYVAL
#error "Only 64-bit machines supported. USE_FLOAT8_BYVAL must be set"
#endif

/*
 * Hold mapping data from requested fields and their TSF and Postgres state
 */
typedef struct ColumnMapping
{
  char columnName[NAMEDATALEN];
  uint32 columnIndex;
  Oid columnTypeId;
  Oid columnArrayTypeId;

  int tsfIdx;

} ColumnMapping;

/*
 * TsfFdwExecState keeps foreign data wrapper specific execution state that we
 * create and hold onto when executing the query.
 */
typedef struct TsfFdwExecState
{
  int idColumnIndex;
  int columnCount;
  struct ColumnMapping *columnMapping;

  int sourceId;
  tsf_file *tsf;
  tsf_iter* iter;

  List	   *qualList;

  // iter to be driven by a specific set of IDs
  int idListIdx;
  int *idList;
  int idListCount;

  // Parsed out of qualList is restriciton info
} TsfFdwExecState;

// case insenstive string comapre
static int stricmp(char const *a, char const *b)
{
  for (;; a++, b++) {
    int d = tolower(*a) - tolower(*b);
    if (d != 0 || !*a)
      return d;
  }
  return -1; //should never be reached
}

/* Local functions forward declarations */
static StringInfo OptionNamesString(Oid currentContextId);
static void TsfGetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel,
                                 Oid foreignTableId);
static void TsfGetForeignPaths(PlannerInfo *root, RelOptInfo *baserel,
                               Oid foreignTableId);
static ForeignScan *TsfGetForeignPlan(PlannerInfo *root, RelOptInfo *baserel,
                                      Oid foreignTableId, ForeignPath *bestPath,
                                      List *targetList,
                                      List *restrictionClauses);
static List *ColumnList(RelOptInfo *baserel);

static void TsfExplainForeignScan(ForeignScanState *scanState,
                                  ExplainState *explainState);
static void TsfBeginForeignScan(ForeignScanState *scanState, int executorFlags);
static TupleTableSlot *TsfIterateForeignScan(ForeignScanState *scanState);
static void TsfEndForeignScan(ForeignScanState *scanState);

static void TsfReScanForeignScan(ForeignScanState *scanState);
static TsfFdwOptions *TsfGetOptions(Oid foreignTableId);
static char *TsfGetOptionValue(Oid foreignTableId, const char *optionName);

static void BuildColumnMapping(Oid foreignTableId, List *columnList, tsf_source *tsfSource,
                               TsfFdwExecState *executionState);

static void FillTupleSlot(tsf_iter *iter, ColumnMapping *columnMappings, Datum *columnValues,
                          bool *columnNulls, int32 idColumnIndex);
static Datum ColumnValue(tsf_v value, tsf_field *f, Oid columnTypeId);
static Datum ColumnValueArray(tsf_v value, tsf_field *f, Oid valueTypeId);

/* declarations for dynamic loading */
PG_MODULE_MAGIC;

#define GET_STR(textp) DatumGetCString(DirectFunctionCall1(textout, PointerGetDatum(textp)))
#define GET_TEXT(cstrp) DatumGetTextP(DirectFunctionCall1(textin, CStringGetDatum(cstrp)))

PG_FUNCTION_INFO_V1(tsf_generate_schemas);

static const char* psql_type(tsf_value_type type)
{
  switch(type) {
    case TypeInt32: return "integer";
    case TypeInt64: return "bigint";
    case TypeFloat32: return "real";
    case TypeFloat64: return "double precision";
    case TypeBool: return "boolean";
    case TypeString: return "text";
    case TypeEnum: return "text";
    case TypeInt32Array: return "integer[]";
    case TypeFloat32Array: return "real[]";
    case TypeFloat64Array: return "double precision[]";
    case TypeBoolArray: return "boolean[]";
    case TypeStringArray: return "text[]";
    case TypeEnumArray: return "text[]";
    case TypeUnkown: return "";
  }
}

static void create_table_schema(stringbuilder *str, const char *prefix, const char *fileName,
                                int sourceId, tsf_source *s, tsf_field_type fieldType,
                                const char *tableSuffix)
{
  char *buf = 0;

  // Locus attr field table
  bool foundOne = false;
  for (int i = 0; i < s->field_count; i++) {
    if (s->fields[i].field_type == fieldType) {
      if (!foundOne) {
        foundOne = true;
        if (str->pos > 0)  // Add double space between consecutive statements
          sb_append_str(str, "\n");

        asprintf(&buf, "CREATE FOREIGN TABLE %s_%d%s (\n       _id integer", prefix, sourceId,
                 tableSuffix);
        sb_append_str(str, buf);
        free(buf);
        if (fieldType == FieldMatrix)
          sb_append_str(str, ",\n       _entity_id integer");
      }
      asprintf(&buf, ",\n       %s %s", s->fields[i].symbol, psql_type(s->fields[i].value_type));
      sb_append_str(str, buf);
      free(buf);
    }
  }
  if (foundOne) {
    sb_append_str(str, "\n       )\n       SERVER tsf_server\n");
    asprintf(&buf, "       OPTIONS (filename '%s', sourceid '%d');\n", fileName, sourceId);
    sb_append_str(str, buf);
    free(buf);
  }
}

/*
 * tsf_generate_schemas is a user-facing helper function to generate
 * CREATE FOREIGN TABLE schemas for all sources in a TSF.
 */
Datum tsf_generate_schemas(PG_FUNCTION_ARGS)
{
  text *prefixText = PG_GETARG_TEXT_P(0);
  text *fileNameText = PG_GETARG_TEXT_P(1);
  const char *prefix = GET_STR(prefixText);
  const char *fileName = GET_STR(fileNameText);
  int sourceId = PG_GETARG_INT32(2);

  tsf_file *tsf = tsf_open_file(fileName);
  if (tsf->errmsg != NULL) {
    ereport(ERROR,
            (errmsg("could not open to %s", fileName),
             errhint("TSF driver connection error: %s", tsf->errmsg)));
  }

  stringbuilder* str = sb_new();

  for(int id = sourceId <= 0 ? 1 : sourceId;
      id < (sourceId <= 0 ? tsf->source_count + 1 : sourceId + 1);
      id++)
  {
    tsf_source* s = &tsf->sources[id - 1];
    create_table_schema(str, prefix, fileName, id, s, FieldLocusAttribute, "");
    create_table_schema(str, prefix, fileName, id, s, FieldMatrix, "_matrix");
    create_table_schema(str, prefix, fileName, id, s, FieldEntityAttribute, "_entity");

  }
  text* ret = GET_TEXT(sb_cstring(str));
  sb_destroy(str, true);
  tsf_close_file(tsf);

  PG_RETURN_TEXT_P(ret);
}


PG_FUNCTION_INFO_V1(tsf_fdw_handler);
PG_FUNCTION_INFO_V1(tsf_fdw_validator);

/*
 * tsf_fdw_handler creates and returns a struct with pointers to foreign table
 * callback functions.
 */
Datum tsf_fdw_handler(PG_FUNCTION_ARGS) {
  FdwRoutine *fdwRoutine = makeNode(FdwRoutine);

  fdwRoutine->GetForeignRelSize = TsfGetForeignRelSize;
  fdwRoutine->GetForeignPaths = TsfGetForeignPaths;
  fdwRoutine->GetForeignPlan = TsfGetForeignPlan;
  fdwRoutine->ExplainForeignScan = TsfExplainForeignScan;

  fdwRoutine->BeginForeignScan = TsfBeginForeignScan;
  fdwRoutine->IterateForeignScan = TsfIterateForeignScan;
  fdwRoutine->ReScanForeignScan = TsfReScanForeignScan;
  fdwRoutine->EndForeignScan = TsfEndForeignScan;
  // fdwRoutine->AnalyzeForeignTable = TsfAnalyzeForeignTable;

  PG_RETURN_POINTER(fdwRoutine);
}

/*
 * tsf_fdw_validator validates options given to one of the following commands:
 * foreign data wrapper, server, user mapping, or foreign table. This function
 * errors out if the given option name or its value is considered invalid.
 */
Datum tsf_fdw_validator(PG_FUNCTION_ARGS) {
  int32 sourceId = -1;

  Datum optionArray = PG_GETARG_DATUM(0);
  Oid optionContextId = PG_GETARG_OID(1);
  List *optionList = untransformRelOptions(optionArray);
  ListCell *optionCell = NULL;

  foreach (optionCell, optionList) {
    DefElem *optionDef = (DefElem *)lfirst(optionCell);
    char *optionName = optionDef->defname;
    bool optionValid = false;

    int32 optionIndex = 0;
    for (optionIndex = 0; optionIndex < ValidOptionCount; optionIndex++) {
      const TsfValidOption *validOption = &(ValidOptionArray[optionIndex]);

      if ((optionContextId == validOption->optionContextId) &&
          (strncmp(optionName, validOption->optionName, NAMEDATALEN) == 0)) {
        optionValid = true;
        break;
      }
    }

    /* if invalid option, display an informative error message */
    if (!optionValid) {
      StringInfo optionNamesString = OptionNamesString(optionContextId);

      ereport(ERROR, (errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
                      errmsg("invalid option \"%s\"", optionName),
                      errhint("Valid options in this context are: %s",
                              optionNamesString->data)));
    }

    /* if port option is given, error out if its value isn't an integer */
    if (strncmp(optionName, OPTION_NAME_SOURCEID, NAMEDATALEN) == 0) {
      char *optionValue = defGetString(optionDef);
      sourceId = pg_atoi(optionValue, sizeof(int32), 0);
      (void) sourceId; // remove warning
    }
  }

  PG_RETURN_VOID();
}

/*
 * OptionNamesString finds all options that are valid for the current context,
 * and concatenates these option names in a comma separated string.
 */
static StringInfo OptionNamesString(Oid currentContextId) {
  StringInfo optionNamesString = makeStringInfo();
  bool firstOptionPrinted = false;

  int32 optionIndex = 0;
  for (optionIndex = 0; optionIndex < ValidOptionCount; optionIndex++) {
    const TsfValidOption *validOption = &(ValidOptionArray[optionIndex]);

    /* if option belongs to current context, append option name */
    if (currentContextId == validOption->optionContextId) {
      if (firstOptionPrinted) {
        appendStringInfoString(optionNamesString, ", ");
      }

      appendStringInfoString(optionNamesString, validOption->optionName);
      firstOptionPrinted = true;
    }
  }

  return optionNamesString;
}

/*
 * TsfGetForeignRelSize obtains relation size estimates for tsf foreign table.
 */
static void TsfGetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel,
                                 Oid foreignTableId) {
  //elog(INFO, "entering function %s", __func__);

  // TODO: use extractRestrictions and id checks to have different estimates

  List *rowClauseList = baserel->baserestrictinfo;
  double rowSelectivity = clauselist_selectivity(root, rowClauseList, 0, JOIN_INNER, NULL);

  TsfFdwOptions *tsfFdwOptions = TsfGetOptions(foreignTableId);
  tsf_file *tsf = tsf_open_file(tsfFdwOptions->filename);
  if (tsf->errmsg != NULL) {
    ereport(ERROR,
            (errmsg("could not open to %s:%d", tsfFdwOptions->filename, tsfFdwOptions->sourceId),
             errhint("TSF driver connection error: %s", tsf->errmsg)));
  }
  Assert(tsfFdwOptions->sourceId <= tsf->source_count);
  tsf_source *tsfSource = &tsf->sources[tsfFdwOptions->sourceId - 1];
  int rowCount = tsfSource->fields[0].field_type == FieldEntityAttribute ? tsfSource->entity_count
                                                                        : tsfSource->locus_count;
  tsf_close_file(tsf);
  double outputRowCount = clamp_row_est(rowCount * rowSelectivity);
  baserel->rows = outputRowCount;
}

/*
 * TsfGetForeignPaths creates possible access paths for a scan on the foreign
 * table. We currently have one possible access path. This path filters out row
 * blocks that are refuted by where clauses, and only returns values for the
 * projected columns.
 */
static void TsfGetForeignPaths(PlannerInfo *root, RelOptInfo *baserel,
                               Oid foreignTableId) {
  //elog(INFO, "entering function %s", __func__);
  Path *foreignScanPath = NULL;

  // TODO: Use the findPaths methdo from multiCorn, adding the tuple
  // ("_id", 1) and when appropriae ("_enitty_id", _rowCount)

  /*
   * We skip reading columns that are not in query. Here we assume that all
   * columns in relation have the same width.
   */
  List *queryColumnList = ColumnList(baserel);
  uint32 queryColumnCount = list_length(queryColumnList);
  Cost startupCost = 10;
  Cost totalCost = (baserel->rows * queryColumnCount) + startupCost;


  // TODO: We can gurantee ordering by ID, so should set 'pathkeys'
  // appropriately

  /* create a foreign path node and add it as the only possible path */
  foreignScanPath =
      (Path *)create_foreignscan_path(root, baserel, baserel->rows, startupCost,
                                      totalCost,
                                      NIL, /* no known ordering */
                                      NULL,           /* not parameterized */
                                      NIL);           /* no fdw_private */

  add_path(baserel, foreignScanPath);
}

static bool isIdRestriction(Oid foreignTableId, MulticornBaseQual *qual)
{
  char *columnName = get_relid_attribute_name(foreignTableId, qual->varattno);

  if (stricmp(columnName, "_id") == 0 &&
          strcmp(qual->opname, "=") == 0)
  {
    // Scalar or useOr
    return !qual->isArray || qual->useOr;
  }
  return false;
}

static bool isInternableRestriction(Oid foreignTableId, MulticornBaseQual *qual)
{
  char *columnName = get_relid_attribute_name(foreignTableId, qual->varattno);
  // TODO: check if clause is a expression that TSF can handle internally.
  // For example:
  // tsf_variable <supported_operator> sub_expression
  return false; // Not yet implemented
}

/*
 * TsfGetForeignPlan creates a foreign scan plan node for scanning the
 * TSF table. We also add the query column list to scan nodes private
 * list, because we need it later for skipping over unused columns in the
 * query.
 */
static ForeignScan *TsfGetForeignPlan(PlannerInfo *root, RelOptInfo *baserel,
                                      Oid foreignTableId, ForeignPath *bestPath,
                                      List *targetList, List *scanClauses) {
  Index scanRangeTableIndex = baserel->relid;
  ForeignScan *foreignScan = NULL;
  ListCell *lc = NULL;
  List *columnList = NIL;
  List *foreignPrivateList = NIL;
  List *localExprs = NIL;
  List *tsfExprs = NIL;

  //elog(INFO, "entering function %s", __func__);
  /*
   * Separate the scan_clauses into those that can be executed
   * internally and those that can't.
   */
  bool foundIdRestriction = false;
  foreach (lc, scanClauses) {
    RestrictInfo *rinfo = (RestrictInfo *)lfirst(lc);

    Assert(IsA(rinfo, RestrictInfo));

    /* Ignore any pseudoconstants, they're dealt with elsewhere */
    if (rinfo->pseudoconstant)
      continue;

    List *quals = NIL;
    extractRestrictions(baserel->relids, rinfo->clause, &quals);

    if(list_length(quals) == 1) {
      MulticornBaseQual *qual = (MulticornBaseQual *)linitial(quals);
      // ID restrictions must be only internalized expressions if they
      // are found. Otherwise we can have any number of internalizable
      // restriction expressions.
      if (!tsfExprs &&
          isIdRestriction(foreignTableId, qual)) {
        tsfExprs = lappend(tsfExprs, rinfo->clause);
        foundIdRestriction = true;
      } else if (!foundIdRestriction &&
                 isInternableRestriction(foreignTableId, qual)) {
        tsfExprs = lappend(tsfExprs, rinfo->clause);
      } else {
        localExprs = lappend(localExprs, rinfo->clause); //We don't hanle
      }
    } else {
      // Wasn't able to extract these restrictions...
      //elog(INFO, "Exracted %d quals", list_length(quals));
      localExprs = lappend(localExprs, rinfo->clause);
    }
  }

  /*
   * As an optimization, we only read columns that are present in the query.
   * To find these columns, we need baserel. We don't have access to baserel
   * in executor's callback functions, so we get the column list here and put
   * it into foreign scan node's private list.
   */
  columnList = ColumnList(baserel);
  foreignPrivateList = list_make1(columnList);


  /* create the foreign scan node */
  foreignScan = make_foreignscan(targetList,
                                 localExprs, /* postgres will run these restrictions on results */
                                 scanRangeTableIndex,
                                 tsfExprs,
                                 foreignPrivateList);
  return foreignScan;
}

/*
 * ColumnList takes in the planner's information about this foreign table. The
 * function then finds all columns needed for query execution, including those
 * used in projections, joins, and filter clauses, de-duplicates these columns,
 * and returns them in a new list. This function is unchanged from mongo_fdw.
 */
static List *ColumnList(RelOptInfo *baserel) {
  List *columnList = NIL;
  List *neededColumnList = NIL;
  AttrNumber columnIndex = 1;
  AttrNumber columnCount = baserel->max_attr;
  List *targetColumnList = baserel->reltargetlist;
  List *restrictInfoList = baserel->baserestrictinfo;
  ListCell *restrictInfoCell = NULL;

  /* first add the columns used in joins and projections */
  neededColumnList = list_copy(targetColumnList);

  /* then walk over all restriction clauses, and pull up any used columns */
  foreach (restrictInfoCell, restrictInfoList) {
    RestrictInfo *restrictInfo = (RestrictInfo *)lfirst(restrictInfoCell);
    Node *restrictClause = (Node *)restrictInfo->clause;
    List *clauseColumnList = NIL;

    /* recursively pull up any columns used in the restriction clause */
    clauseColumnList = pull_var_clause(restrictClause, PVC_RECURSE_AGGREGATES,
                                       PVC_RECURSE_PLACEHOLDERS);

    neededColumnList = list_union(neededColumnList, clauseColumnList);
  }

  /* walk over all column definitions, and de-duplicate column list */
  for (columnIndex = 1; columnIndex <= columnCount; columnIndex++) {
    ListCell *neededColumnCell = NULL;
    Var *column = NULL;

    /* look for this column in the needed column list */
    foreach (neededColumnCell, neededColumnList) {
      Var *neededColumn = (Var *)lfirst(neededColumnCell);
      if (neededColumn->varattno == columnIndex) {
        column = neededColumn;
        break;
      }
    }

    if (column != NULL) {
      columnList = lappend(columnList, column);
    }
  }

  return columnList;
}

/*
 * TsfExplainForeignScan produces extra output for the Explain command.
 */
static void TsfExplainForeignScan(ForeignScanState *scanState,
                                  ExplainState *explainState) {
  Oid foreignTableId = RelationGetRelid(scanState->ss.ss_currentRelation);
  TsfFdwOptions *tsfFdwOptions = TsfGetOptions(foreignTableId);

  ExplainPropertyText("Tsf File", tsfFdwOptions->filename, explainState);
  ExplainPropertyLong("Tsf Source ID", tsfFdwOptions->sourceId, explainState);

  /* supress file size if we're not showing cost details */
  if (explainState->costs) {
    struct stat statBuffer;

    int statResult = stat(tsfFdwOptions->filename, &statBuffer);
    if (statResult == 0) {
      ExplainPropertyLong("TSF File Size", (long)statBuffer.st_size,
                          explainState);
    }
  }
}

/*
 * TsfBeginForeignScan opens the TSF file. Does not validate the query
 * yet, that happens of the first iteration.
 */
static void TsfBeginForeignScan(ForeignScanState *scanState,
                                int executorFlags) {
  /* From FDW docs:
   *
   * Begin executing a foreign scan. This is called during executor startup.
   * It should perform any initialization needed before the scan can start,
   * but not start executing the actual scan (that should be done upon the
   * first call to IterateForeignScan). The ForeignScanState node has
   * already been created, but its fdw_state field is still NULL.
   * Information about the table to scan is accessible through the
   * ForeignScanState node (in particular, from the underlying ForeignScan
   * plan node, which contains any FDW-private information provided by
   * GetForeignPlan). eflags contains flag bits describing the executor's
   * operating mode for this plan node.
   *
   * Note that when (eflags & EXEC_FLAG_EXPLAIN_ONLY) is true, this function
   * should not perform any externally-visible actions; it should only do
   * the minimum required to make the node state valid for
   * ExplainForeignScan and EndForeignScan.
   *
   */

  /* if Explain with no Analyze, do nothing */
  if (executorFlags & EXEC_FLAG_EXPLAIN_ONLY) {
    return;
  }

  Oid foreignTableId = RelationGetRelid(scanState->ss.ss_currentRelation);
  TsfFdwOptions *tsfFdwOptions = TsfGetOptions(foreignTableId);

  tsf_file *tsf = tsf_open_file(tsfFdwOptions->filename);
  if(tsf->errmsg != NULL) {
    ereport(ERROR,
            (errmsg("could not open to %s:%d", tsfFdwOptions->filename,
                    tsfFdwOptions->sourceId),
             errhint("TSF driver connection error: %s",tsf->errmsg)));

  }
  Assert(tsfFdwOptions->sourceId <= tsf->source_count);
  tsf_source* tsfSource = &tsf->sources[tsfFdwOptions->sourceId-1];

  /* create column mapping */
  ForeignScan* foreignScan = (ForeignScan *)scanState->ss.ps.plan;

  List* foreignPrivateList = foreignScan->fdw_private;
  Assert(list_length(foreignPrivateList) == 1);

  List *columnList = (List *)linitial(foreignPrivateList);

  /* create and set foreign execution state */
  TsfFdwExecState *executionState = (TsfFdwExecState *)palloc0(sizeof(TsfFdwExecState));
  BuildColumnMapping(foreignTableId, columnList, tsfSource, executionState);

  /* We extract the qual list, but execute it one first iteration through ForeingScan */
  executionState->qualList = NULL;

  List* foreignExprs = foreignScan->fdw_exprs;
  ListCell *lc = NULL;
  foreach(lc, foreignExprs)
  {
    extractRestrictions(bms_make_singleton(foreignScan->scan.scanrelid),
                        ((Expr *) lfirst(lc)),
                        &executionState->qualList);
  }

  executionState->tsf = tsf;
  executionState->sourceId = tsfFdwOptions->sourceId;
  executionState->iter = NULL;

  scanState->fdw_state = (void *)executionState;
}

static void executeQualList(ForeignScanState *scanState)
{
  TsfFdwExecState *state = (TsfFdwExecState *)scanState->fdw_state;
  ListCell   *lc;

  ExprContext *econtext = scanState->ss.ps.ps_ExprContext;

  foreach(lc, state->qualList)
  {
    MulticornBaseQual *qual = lfirst(lc);
    MulticornConstQual *newqual = NULL;
    bool		isNull;
    ExprState  *expr_state = NULL;

    switch (qual->right_type) {
      case T_Param:
        expr_state = ExecInitExpr(((MulticornParamQual *)qual)->expr, (PlanState *)scanState);
        newqual = palloc0(sizeof(MulticornConstQual));
        newqual->base.right_type = T_Const;
        newqual->base.varattno = qual->varattno;
        newqual->base.opname = qual->opname;
        newqual->base.isArray = qual->isArray;
        newqual->base.useOr = qual->useOr;
        newqual->value = ExecEvalExpr(expr_state, econtext, &isNull, NULL);
        newqual->base.typeoid = qual->typeoid;
        newqual->isnull = isNull;
        break;
      case T_Const:
        newqual = (MulticornConstQual *)qual;
        break;
      default:
        break;
    }
    if (newqual != NULL) {
      // Process const qual into TSF based query state manager.
      // Special case for _id and _entity_id
      if (state->idColumnIndex >= 0 &&
          newqual->base.varattno - 1 == state->idColumnIndex) {
        // _id qual
        state->idListIdx = 0;
        if(newqual->isnull){
          state->idList = (void*)1; // non null
          state->idListCount = 0;
        }else if(!newqual->isnull && newqual->base.isArray ) {
          ArrayType* array = DatumGetArrayTypeP(newqual->value);
          Oid elmtype = ARR_ELEMTYPE(array);
          Datum *dvalues;
          bool *dnulls;
          int nelems;
          int16 elmlen;
          bool elmbyval;
          char elmalign;
          get_typlenbyvalalign(elmtype, &elmlen, &elmbyval, &elmalign);
          deconstruct_array(array, elmtype, elmlen, elmbyval, elmalign,
                            &dvalues, &dnulls, &nelems);
          state->idList = calloc(sizeof(int), nelems);
          state->idListCount = 0;
          for(int i=0; i<nelems; i++) {
            if (!dnulls[i])
              state->idList[state->idListCount++] = DatumGetInt32(dvalues[i]);
          }
        }else{
          state->idList = calloc(sizeof(int), 1);
          state->idListCount = 1;
          // assert type?
          state->idList[0] = DatumGetInt32(newqual->value);
        }
      }
    }
  }
}

/*
 * TsfIterateForeignScan reads the next record from TSF, converts it to
 * a PostgreSQL tuple, and stores the converted tuple into the ScanTupleSlot as
 * a virtual tuple.
 */
static TupleTableSlot *TsfIterateForeignScan(ForeignScanState *scanState) {
  TsfFdwExecState *state = (TsfFdwExecState *)scanState->fdw_state;

  TupleTableSlot *tupleSlot = scanState->ss.ss_ScanTupleSlot;
  TupleDesc tupleDescriptor = tupleSlot->tts_tupleDescriptor;
  Datum *columnValues = tupleSlot->tts_values;
  bool *columnNulls = tupleSlot->tts_isnull;
  int32 columnCount = tupleDescriptor->natts;

  /*
   * We execute the protocol to load a virtual tuple into a slot. We
   * first call ExecClearTuple, then fill in values / isnull arrays, and
   * last call ExecStoreVirtualTuple. If our iter is done, we just return
   * an empty slot as required.
   */
  ExecClearTuple(tupleSlot);
  memset(columnValues, 0, columnCount * sizeof(Datum));
  memset(columnNulls, true, columnCount * sizeof(bool));

  if (!state->iter) {
    /* initialize all values for this row to null */

    // Evalue expressions we extracted in qualList
    executeQualList(scanState);

    // TODO: executeQualList will set state->entityIdxList,
    // state->entityIdxCount;
    int *fields = calloc(sizeof(int), state->columnCount);
    for (int i = 0; i < state->columnCount; i++)
      fields[i] = state->columnMapping[i].tsfIdx;
    state->iter =
        tsf_query_table(state->tsf, state->sourceId, state->columnCount, fields, -1, NULL);
    free(fields);
    if (!state->iter) {
      ereport(ERROR, (errmsg("Failed to start TSF table query"),
                      errhint("Query failed on %d fields", state->columnCount)));
    }
  }

  if (state->idList) {
    for(int i=0; i<state->idListCount; i++)
    // Special iteration if we are doing ID lookups
    if (state->idListIdx < state->idListCount &&
        tsf_iter_id(state->iter, state->idList[state->idListIdx++])) {
      FillTupleSlot(state->iter, state->columnMapping, columnValues, columnNulls, state->idColumnIndex);
      ExecStoreVirtualTuple(tupleSlot);
    }
  } else {
    // TODO: Have special iteration mode with quals:
    // - One table iterator per qual. For each condition, advance one, do
    // - condition test. If true, do second test with ID iterator. Short
    // - circut if fail. (i.e if first condition does most filtering,
    // - most other filters are not run).
    // - still need to learn how OR compound statements work at this level

    if (tsf_iter_next(state->iter)) {
      FillTupleSlot(state->iter, state->columnMapping, columnValues, columnNulls, state->idColumnIndex);
      ExecStoreVirtualTuple(tupleSlot);
    }
  }

  // Note if not iter succeeded, then the cleared slot is a sentinal to
  // stop iteration.

  return tupleSlot;
}

/*
 * TsfEndForeignScan finishes scanning the foreign table, closes the cursor
 * and the connection to TSF, and reclaims scan related resources.
 */
static void TsfEndForeignScan(ForeignScanState *scanState) {
  TsfFdwExecState *state = (TsfFdwExecState *)scanState->fdw_state;

  /* if we executed a query, reclaim tsf related resources */
  if (state != NULL) {
    tsf_iter_close(state->iter);
    tsf_close_file(state->tsf);
    free(state->idList);
  }
}

/*
 * TsfReScanForeignScan rescans the foreign table. We can re-use some
 * state and open handles.
 */
static void TsfReScanForeignScan(ForeignScanState *scanState) {
  elog(DEBUG1, "entering function %s", __func__);

  TsfFdwExecState *state = (TsfFdwExecState *)scanState->fdw_state;
  tsf_iter* iter = state->iter;

  int fieldCount = state->columnCount;
  int* fields = calloc(sizeof(int), fieldCount);
  for (int i = 0; i < state->columnCount; i++)
    fields[i] = state->columnMapping[i].tsfIdx;

  // New iter
  tsf_iter* new_iter = tsf_query_table(state->tsf, iter->source_id, fieldCount, fields, -1, NULL);
  free(fields);

  tsf_iter_close(iter);

  state->iter = new_iter;
}

/*
 * TsfGetOptions returns the option values to be used when connecting to
 * and querying TSF. To resolve these values, the function checks the
 * foreign table's options.
 */
static TsfFdwOptions *TsfGetOptions(Oid foreignTableId) {
  TsfFdwOptions *tsfFdwOptions = NULL;
  char *filename = NULL;
  char *sourceIdName = NULL;
  int32 sourceId = 0;

  filename = TsfGetOptionValue(foreignTableId, OPTION_NAME_FILENAME);
  sourceIdName = TsfGetOptionValue(foreignTableId, OPTION_NAME_SOURCEID);
  sourceId = pg_atoi(sourceIdName, sizeof(int32), 1);

  tsfFdwOptions = (TsfFdwOptions *)palloc0(sizeof(TsfFdwOptions));
  tsfFdwOptions->filename = filename;
  tsfFdwOptions->sourceId = sourceId;

  return tsfFdwOptions;
}

/*
 * TsfGetOptionValue walks over foreign table and foreign server options, and
 * looks for the option with the given name. If found, the function returns the
 * option's value.
 */
static char *TsfGetOptionValue(Oid foreignTableId, const char *optionName) {
  ForeignTable *foreignTable = NULL;
  ForeignServer *foreignServer = NULL;
  List *optionList = NIL;
  ListCell *optionCell = NULL;
  char *optionValue = NULL;

  foreignTable = GetForeignTable(foreignTableId);
  foreignServer = GetForeignServer(foreignTable->serverid);

  optionList = list_concat(optionList, foreignTable->options);
  optionList = list_concat(optionList, foreignServer->options);

  foreach (optionCell, optionList) {
    DefElem *optionDef = (DefElem *)lfirst(optionCell);
    char *optionDefName = optionDef->defname;

    if (strncmp(optionDefName, optionName, NAMEDATALEN) == 0) {
      optionValue = defGetString(optionDef);
      break;
    }
  }

  return optionValue;
}

/*
 * BuildColumnMapping pingHash creates a array of ColumnMapping objects
 * the length of observed column variables (in columnList). Each mapping
 * has the index into the values/nulls array it should fill the tuple
 * (note this is varattno - 1).
 */
static void BuildColumnMapping(Oid foreignTableId, List *columnList, tsf_source *tsfSource,
                               TsfFdwExecState *executionState)
{
  ListCell *columnCell = NULL;
  int length = list_length(columnList);
  executionState->columnMapping = palloc0(sizeof(ColumnMapping) * length);
  executionState->columnCount = 0;
  executionState->idColumnIndex = -1;

  foreach (columnCell, columnList) {
    Var *column = (Var *)lfirst(columnCell);
    AttrNumber columnId = column->varattno;
    char *columnName = get_relid_attribute_name(foreignTableId, columnId);

    // Sentinal fields (not in backend table, provided by iter data)
    if (stricmp(columnName, "_id") == 0) {
      executionState->idColumnIndex = columnId - 1;
      continue;
    }

    ColumnMapping *columnMapping = &executionState->columnMapping[executionState->columnCount];
    executionState->columnCount++;
    columnMapping->tsfIdx = -1;

    // Find this coloumn by its symbol name in the source
    for (int j = 0; j < tsfSource->field_count; j++) {
      if (stricmp(tsfSource->fields[j].symbol, columnName) == 0) {
        columnMapping->tsfIdx = j;
        break;
      }
    }

    if (columnMapping->tsfIdx < 0) {
      ereport(ERROR, (errmsg("Cound not find referenced field in TSF table"),
                      errhint("Field in query: %s", columnName)));
    }

    columnMapping->columnIndex = columnId - 1;
    columnMapping->columnTypeId = column->vartype;
    columnMapping->columnArrayTypeId = get_element_type(column->vartype);
  }
}

/*
 * FillTupleSlot walks over the fields in our executed iterator. For each
 * field it converts it to the PostgreSQL Datum type and places it in
 * columnValues at the appropriate index.
 */
static void FillTupleSlot(tsf_iter *iter, ColumnMapping *columnMappings, Datum *columnValues,
                          bool *columnNulls, int32 idColumnIndex)
{
  for (int i = 0; i<iter->field_count; i++)
  {
    ColumnMapping *columnMapping = &columnMappings[i];
    Oid columnTypeId = columnMapping->columnTypeId;
    Oid columnArrayTypeId = columnMapping->columnArrayTypeId;
    tsf_field* f = iter->fields[i];
    tsf_v value = iter->cur_values[i];
    bool is_null  = iter->cur_nulls[i];
    int32 columnIndex = columnMapping->columnIndex;

    /* fill in corresponding column value and null flag */
    if (OidIsValid(columnArrayTypeId)) {
      columnValues[columnIndex] = ColumnValueArray(value, f, columnArrayTypeId);
      columnNulls[columnIndex] = false;
    } else {
      columnNulls[columnIndex] = is_null;
      if (!is_null)
        columnValues[columnIndex] = ColumnValue(value, f, columnTypeId);
    }
  }
  if (idColumnIndex >= 0) {
    columnValues[idColumnIndex] = Int32GetDatum(iter->cur_record_id);
    columnNulls[idColumnIndex] = false;
  }
}

/*
 * ColumnValueArray uses array element type id to read the current array pointed
 * to by the BSON iterator, and converts each array element (with matching type)
 * to the corresponding PostgreSQL datum. Then, the function constructs an array
 * datum from element datums, and returns the array datum.
 */
static Datum ColumnValueArray(tsf_v value, tsf_field *f, Oid valueTypeId) {
  int size = va_size(value);
  Datum *columnValueArray = palloc0(size * sizeof(Datum));
  bool typeMatched = false;
  switch (valueTypeId) {
    case INT4OID:
      if (f->value_type == TypeInt32Array) {
        typeMatched = true;
        for(int i=0; i< size; i++)
          columnValueArray[i] = Int32GetDatum(va_int32(value, i));
      }
      break;
    case FLOAT4OID:
      if (f->value_type == TypeFloat32Array) {
        typeMatched = true;
        for(int i=0; i< size; i++)
          columnValueArray[i] = Float4GetDatum(va_float32(value, i));
      }
      break;
    case FLOAT8OID:
      if (f->value_type == TypeFloat64Array) {
        typeMatched = true;
        for(int i=0; i< size; i++)
          columnValueArray[i] = Float8GetDatum(va_float64(value, i));
      }
      break;
    case BOOLOID:
      if (f->value_type == TypeBoolArray) {
        typeMatched = true;
        for(int i=0; i< size; i++)
          columnValueArray[i] = BoolGetDatum(va_bool(value, i));
      }
      break;
    case TEXTOID:
      if (f->value_type == TypeStringArray) {
        typeMatched = true;
        const char* s = va_array(value);
        for(int i=0; i< size; i++) {
          columnValueArray[i] = CStringGetTextDatum(s);
          // Advanced past next NULL
          while (s[0] != '\0')  // increment to next NULL
            s++;
          s++;  // go past the NULL
        }
      } else if (f->value_type == TypeEnumArray) {
        typeMatched = true;
        for(int i=0; i< size; i++) {
          const char* s = va_enum_as_str(value, i, f->enum_names);
          if(s)
            columnValueArray[i] = CStringGetTextDatum(s);
          else
            columnValueArray[i] = CStringGetTextDatum("");
        }
      }
      break;
    default:
      break; // typeMatched will be false
  }
  if(!typeMatched) {
    ereport(ERROR, (errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
                    errmsg("cannot convert tsf array type to column type"),
                    errhint("Array value type: %u, Tsf type: %d", (uint32)valueTypeId, f->value_type)));
    return 0; //NULL Datum
  }

  bool typeByValue = false;
  char typeAlignment = 0;
  int16 typeLength = 0;
  get_typlenbyvalalign(valueTypeId, &typeLength, &typeByValue, &typeAlignment);
  ArrayType *columnValueObject = construct_array(columnValueArray, size, valueTypeId,
                                      typeLength, typeByValue, typeAlignment);

  return PointerGetDatum(columnValueObject);
}

/*
 * ColumnValue uses reads data from the TSF value into a PostgreSQL datum
 * of the provided type.
 *
 * NOTE: This function assumes the foriegn table is constructed with
 * appropriate types for each field (which it will be if the schema is
 * generated by this FDW).
 */
static Datum ColumnValue(tsf_v value, tsf_field *f, Oid columnTypeId)
{
  switch (columnTypeId) {
    case INT4OID:
      if (f->value_type == TypeInt32)
        return Int32GetDatum(v_int32(value));
      break;
    case INT8OID:
      if (f->value_type == TypeInt64)
        return Int64GetDatum(v_int64(value));
      break;
    case FLOAT4OID:
      if (f->value_type == TypeFloat32)
        return Float4GetDatum(v_float32(value));
      break;
    case FLOAT8OID:
      if (f->value_type == TypeFloat64)
        return Float8GetDatum(v_float64(value));
      break;
    case BOOLOID:
      if (f->value_type == TypeBool)
        return BoolGetDatum(v_bool(value));
      break;
    case TEXTOID: {
      const char *str = 0;
      if (f->value_type == TypeEnum)
        str = v_enum_as_str(value, f->enum_names);
      else if (f->value_type == TypeString)
        str = v_str(value);
      if (str)
        return CStringGetTextDatum(str);
      break;
    }
    default: {
      break; //Any fallthrough generates an error
    }
  }
  ereport(ERROR, (errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
                  errmsg("cannot convert tsf type to column type"),
                  errhint("Column type: %u, Tsf type: %d", (uint32)columnTypeId, f->value_type)));

  return 0; //NULL Datum
}
