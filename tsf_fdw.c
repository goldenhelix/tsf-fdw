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
 * The contrib/postgres_fdw also become vital as a reference for how to
 * internalize join restrictions and sort orders to provide decent join
 * performance between TSF foreign tables.
 *
 * Finally, the restriction parsing and evaluation is heavily modeled
 * after the Python FDW module Multicorn libary by Kozea
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
#include "util.h"

#include "access/reloptions.h"
#include "catalog/pg_type.h"
#include "catalog/pg_opfamily.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "commands/vacuum.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "nodes/makefuncs.h"
#include "optimizer/cost.h"
#include "optimizer/paths.h"
#include "optimizer/pathnode.h"
#include "optimizer/plancat.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/var.h"
#include "parser/parsetree.h"
#include "storage/ipc.h"
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

/* declarations for dynamic loading */
PG_MODULE_MAGIC;

/* Schema generation function for a single source */
PG_FUNCTION_INFO_V1(tsf_generate_schemas);

/* FDW handler and options validator functions */
PG_FUNCTION_INFO_V1(tsf_fdw_handler);
PG_FUNCTION_INFO_V1(tsf_fdw_validator);

/* Module load init */
void _PG_init(void);

typedef struct TsfFdwRelationInfo {
  /* baserestrictinfo columns used */
  List *columnList;

  /* Estimated size and cost for a scan with baserestrictinfo quals. */
  int width; /* length of columnList */
  int row_count;
  int rows_with_param_id;
  bool is_matrix_source;
  int rows_with_param_entity_id;
  double rows_selected;
  Cost startup_cost;
  Cost total_cost;
} TsfFdwRelationInfo;

/*
 * For each requested field, we have mapping to TSF source, field,
 * mapping, iterator and Posgres type information.
 */
typedef struct ColumnMapping {
  uint32 columnIndex;
  Oid columnTypeId;
  Oid columnArrayTypeId;

  int sourceIdx;
  int fieldIdx;

  int mappingSourceIdx;
  // mappingFieldIdx is always 0

  tsf_iter *iter;
  tsf_iter *mappingIter;  // Not owned, borrowed from TsfSourceState

} ColumnMapping;

/* Restrictions that are internalized to the FDW */
typedef enum {
  RestrictInt,
  RestrictDouble,
  RestrictEnum,
  RestrictBool,
  RestrictSring
} RestrictionType;

typedef struct RestrictionBase {
  ColumnMapping *col;
  RestrictionType type;
  bool includeMissing;  // Include missing values
} RestrictionBase;

typedef struct IntRestriction {
  RestrictionBase base;
  int lowerBound;
  int upperBound;
  bool includeBounds;  // Operation is >= and <=, not > and <
} IntRestriction;

typedef struct DoubleRestriction {
  RestrictionBase base;
  double lowerBound;
  double upperBound;
  bool includeBounds;  // Operation is >= and <=, not > and <
} DoubleRestriction;

typedef struct EnumRestriction {
  RestrictionBase base;
  int includeCount;
  int *include;  // Enum indexes to include
} EnumRestriction;

typedef struct BoolRestriction {
  RestrictionBase base;
  bool includeTrue;
  bool includeFalse;
} BoolRestriction;

typedef struct StringRestriction {
  RestrictionBase base;
  const char *match;  // Exact match
  bool doesNotMatch;
} StringRestriction;

typedef struct TsfSourceState {
  int sourceId;
  const char *fileName;
  tsf_file *tsf;          // Not owned, borrowed pointer from tsfHandleCache;
  tsf_iter *mappingIter;  // Owned, used if this is a mapping source
} TsfSourceState;

/*
 * TsfFdwExecState keeps foreign data wrapper specific execution state
 * that we create and hold onto when executing the query.
 */
typedef struct TsfFdwExecState {
  // column index here and in columnmapping means index into the tuple
  // representing full width of the FDW table. By convention _id is 0,
  // _entity_id is 1, but technically you can define a FDW table with
  // these fields in any order.
  int idColumnIndex;
  int entityIdColumnIndex;
  int columnCount;
  struct ColumnMapping *columnMapping;

  int restrictionCount;
  struct RestrictionBase *columnRestrictions;

  tsf_field_type fieldType;

  TsfSourceState *sources;
  int sourceCount;

  tsf_iter *iter;  // Primary iterator (no fields)

  // Parsed out of qualList is restriciton info
  List *qualList;

  // iter to be driven by a specific set of IDs
  int idListIdx;
  int *idList;
  int idListCount;

  // Used to set up matrix field query
  int *entityIdList;
  int entityIdListCount;

} TsfFdwExecState;

/* FDW Handler Functions */
static void TsfGetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel, Oid foreignTableId);
static void TsfGetForeignPaths(PlannerInfo *root, RelOptInfo *baserel, Oid foreignTableId);
static ForeignScan *TsfGetForeignPlan(PlannerInfo *root, RelOptInfo *baserel, Oid foreignTableId,
                                      ForeignPath *bestPath, List *targetList,
                                      List *restrictionClauses);

static void TsfExplainForeignScan(ForeignScanState *scanState, ExplainState *explainState);
static void TsfBeginForeignScan(ForeignScanState *scanState, int executorFlags);
static TupleTableSlot *TsfIterateForeignScan(ForeignScanState *scanState);
static void TsfEndForeignScan(ForeignScanState *scanState);

static void TsfReScanForeignScan(ForeignScanState *scanState);

/* Table introspection helpders  */
static TsfFdwOptions *getTsfFdwOptions(Oid foreignTableId);
static List *columnList(RelOptInfo *baserel);
static char *strJoin(const char *left, const char *right, char joiner);
static void buildColumnMapping(Oid foreignTableId, List *columnList, TsfFdwOptions *tsfFdwOptions,
                               TsfFdwExecState *executionState);

/* Plan builder helpers */
static bool isIdRestriction(Oid foreignTableId, MulticornBaseQual *qual);
static bool isInternableRestriction(Oid foreignTableId, MulticornBaseQual *qual);
static bool isEntityIdRestriction(Oid foreignTableId, MulticornBaseQual *qual);
static bool isParamatizable(Oid foreignTableId, RelOptInfo *baserel, Expr *expr,
                            bool *outIsEntityIdRestriction);

/* Scan iteration helpers */
static void executeQualList(ForeignScanState *scanState, bool *updatedEntityIds);
static void initQuery(TsfFdwExecState *state);
static void resetQuery(TsfFdwExecState *state);
static bool iterateWithRestrictions(TsfFdwExecState *state);
static void fillTupleSlot(TsfFdwExecState *state, Datum *columnValues, bool *columnNulls);

/* case insenstive string comapre */
static int stricmp(char const *a, char const *b)
{
  for (;; a++, b++) {
    int d = tolower(*a) - tolower(*b);
    if (d != 0 || !*a)
      return d;
  }
  return -1;  // should never be reached
}

/*
 * Convert a tsf_value_type to appropriate Postgres type
 */
static const char *psqlTypeForTsfType(tsf_value_type type)
{
  switch (type) {
    case TypeInt32:
      return "integer";
    case TypeInt64:
      return "bigint";
    case TypeFloat32:
      return "real";
    case TypeFloat64:
      return "double precision";
    case TypeBool:
      return "boolean";
    case TypeString:
      return "text";
    case TypeEnum:
      return "text";
    case TypeInt32Array:
      return "integer[]";
    case TypeFloat32Array:
      return "real[]";
    case TypeFloat64Array:
      return "double precision[]";
    case TypeBoolArray:
      return "boolean[]";
    case TypeStringArray:
      return "text[]";
    case TypeEnumArray:
      return "text[]";
    case TypeUnkown:
      return "";
  }
}

static const char *fieldTypeLetter(tsf_field_type fieldType)
{
  switch (fieldType) {
    case FieldLocusAttribute:
      return "l";
    case FieldEntityAttribute:
      return "e";
    case FieldMatrix:
      return "m";
    default:
      return "";
  }
}

static void createTableSchema(stringbuilder *str, const char *prefix, const char *fileName,
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
      asprintf(&buf, ",\n       \"%s\" %s", s->fields[i].symbol,
               psqlTypeForTsfType(s->fields[i].value_type));
      sb_append_str(str, buf);
      free(buf);
    }
  }
  if (foundOne) {
    sb_append_str(str, "\n       )\n       SERVER tsf_server\n");
    const char *fieldTypeStr = fieldTypeLetter(fieldType);
    asprintf(&buf, "       OPTIONS (filename '%s', sourceid '%d', fieldtype '%s');\n", fileName,
             sourceId, fieldTypeStr);
    sb_append_str(str, buf);
    free(buf);
  }
}

// Helpers
#define GET_STR(textp) DatumGetCString(DirectFunctionCall1(textout, PointerGetDatum(textp)))
#define GET_TEXT(cstrp) DatumGetTextP(DirectFunctionCall1(textin, CStringGetDatum(cstrp)))

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

  stringbuilder *str = sb_new();

  for (int id = sourceId <= 0 ? 1 : sourceId;
       id < (sourceId <= 0 ? tsf->source_count + 1 : sourceId + 1);
       id++) {
    tsf_source *s = &tsf->sources[id - 1];
    createTableSchema(str, prefix, fileName, id, s, FieldLocusAttribute, "");
    createTableSchema(str, prefix, fileName, id, s, FieldMatrix, "_matrix");
    createTableSchema(str, prefix, fileName, id, s, FieldEntityAttribute, "_entity");
  }
  text *ret = GET_TEXT(sb_cstring(str));
  sb_destroy(str, true);
  tsf_close_file(tsf);

  PG_RETURN_TEXT_P(ret);
}

// Backed process cache that outlives individual queries and holds open
// TSF files by their name.
typedef struct TsfHandleCache {
  tsf_file **tsfs;
  char **tsfFileNames;
  int count;
} TsfHandleCache;

TsfHandleCache *tsfHandleCache;

/*
 * Find an existing open TSF file, or open it and add it to our
 * tsfHandleCache
 */
static tsf_file *tsfCacheOpen(const char *tsfFileName)
{
  for (int i = 0; i < tsfHandleCache->count; i++) {
    if (strcmp(tsfFileName, tsfHandleCache->tsfFileNames[i]) == 0) {
      return tsfHandleCache->tsfs[i];
    }
  }

  // Open new TSF file
  tsf_file *tsf = tsf_open_file(tsfFileName);
  if (tsf->errmsg != NULL) {
    ereport(ERROR,
            (errmsg("could not open %s", tsfFileName),
             errhint("TSF driver connection error: %s", tsf->errmsg)));
  }

  // Expand our lists to include new TSF file
  MemoryContext oldctx = MemoryContextSwitchTo(CacheMemoryContext);
  TsfHandleCache prev = *tsfHandleCache;
  tsfHandleCache->tsfs = palloc0(sizeof(tsf_file *) * (tsfHandleCache->count + 1));
  tsfHandleCache->tsfFileNames = palloc0(sizeof(tsf_file *) * (tsfHandleCache->count + 1));
  if (tsfHandleCache->count > 0) {
    memcpy(tsfHandleCache->tsfs, prev.tsfs, sizeof(tsf_file *) * tsfHandleCache->count);
    memcpy(tsfHandleCache->tsfFileNames, prev.tsfFileNames,
           sizeof(const char *) * tsfHandleCache->count);
    pfree(prev.tsfs);
    pfree(prev.tsfFileNames);
  }
  int idx = tsfHandleCache->count;
  tsfHandleCache->count++;
  tsfHandleCache->tsfs[idx] = tsf;
  // Clone file name as its passed by ref
  tsfHandleCache->tsfFileNames[idx] = palloc0(strlen(tsfFileName) + 1);
  memcpy(tsfHandleCache->tsfFileNames[idx], tsfFileName, strlen(tsfFileName));

  MemoryContextSwitchTo(oldctx);
  return tsf;
}

/**
 * Find a matching TsfSourceState, or add one in executionState
 */
static int getTsfSource(const char *tsfFileName, int sourceId, TsfFdwExecState *executionState)
{
  for (int i = 0; i < executionState->sourceCount; i++) {
    if (strcmp(tsfFileName, executionState->sources[i].fileName) == 0) {
      if (sourceId == executionState->sources[i].sourceId)
        return i;
    }
  }
  Assert(sourceId <= tsf->source_count);

  TsfSourceState *prev = executionState->sources;
  executionState->sources = palloc0(sizeof(TsfSourceState) * (executionState->sourceCount + 1));
  if (executionState->sourceCount > 0) {
    memcpy(executionState->sources, prev, sizeof(TsfSourceState) * executionState->sourceCount);
    pfree(prev);
  }
  int sourceIdx = executionState->sourceCount;
  executionState->sourceCount++;
  TsfSourceState *s = &executionState->sources[sourceIdx];
  s->fileName = tsfFileName;
  s->sourceId = sourceId;
  s->tsf = tsfCacheOpen(tsfFileName);
  return sourceIdx;
}

/*
 * Exit callbakc function to clean up our tsfHandleCache, closing all
 * open file handles etc
 */
static void tsfFdwExit(int code, Datum arg)
{
  if (!tsfHandleCache)
    return;

  for (int i = 0; i < tsfHandleCache->count; i++) {
    tsf_close_file(tsfHandleCache->tsfs[i]);
    pfree(tsfHandleCache->tsfFileNames[i]);
  }
  pfree(tsfHandleCache);
  tsfHandleCache = NULL;
}

/*
 * Library load-time initialization, sets on_proc_exit() callback for
 * backend shutdown.
 */
void _PG_init(void)
{
  MemoryContext oldctx = MemoryContextSwitchTo(CacheMemoryContext);
  tsfHandleCache = palloc0(sizeof(TsfHandleCache));
  MemoryContextSwitchTo(oldctx);

  on_proc_exit(&tsfFdwExit, PointerGetDatum(NULL));
}

/*
 * tsf_fdw_handler creates and returns a struct with pointers to foreign table
 * callback functions.
 */
Datum tsf_fdw_handler(PG_FUNCTION_ARGS)
{
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
 * buildOptionNamesString finds all options that are valid for the current context,
 * and concatenates these option names in a comma separated string.
 */
static StringInfo buildOptionNamesString(Oid currentContextId)
{
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
 * tsf_fdw_validator validates options given to one of the following commands:
 * foreign data wrapper, server, user mapping, or foreign table. This function
 * errors out if the given option name or its value is considered invalid.
 */
Datum tsf_fdw_validator(PG_FUNCTION_ARGS)
{
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
      StringInfo optionNamesString = buildOptionNamesString(optionContextId);

      ereport(ERROR, (errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
                      errmsg("invalid option \"%s\"", optionName),
                      errhint("Valid options in this context are: %s", optionNamesString->data)));
    }

    if (strncmp(optionName, OPTION_NAME_SOURCEID, NAMEDATALEN) == 0) {
      char *optionValue = defGetString(optionDef);
      sourceId = pg_atoi(optionValue, sizeof(int32), 0);
      (void)sourceId;  // remove warning
    }

    if (strncmp(optionName, OPTION_NAME_FIELDTYPE, NAMEDATALEN) == 0) {
      char *optionValue = defGetString(optionDef);
      if ((strncmp(optionValue, "m", NAMEDATALEN) != 0) &&
          (strncmp(optionValue, "l", NAMEDATALEN) != 0) &&
          (strncmp(optionValue, "e", NAMEDATALEN) != 0)) {
        ereport(ERROR, (errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
                        errmsg("invalid value for \"%s\"", optionName),
                        errhint("Valid values are 'm' or matrix, 'l' for locus attribute or 'e' "
                                "for entity attribute.")));
      }
    }
  }

  PG_RETURN_VOID();
}

/*
 * TsfGetForeignRelSize obtains relation size estimates for tsf foreign table.
 */
static void TsfGetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel, Oid foreignTableId)
{
  TsfFdwOptions *tsfFdwOptions = getTsfFdwOptions(foreignTableId);
  const char *tsfFileName = strJoin(tsfFdwOptions->path, tsfFdwOptions->filename, '\0');
  tsf_file *tsf = tsfCacheOpen(tsfFileName);
  if (tsfFdwOptions->sourceId < 1 || tsfFdwOptions->sourceId > tsf->source_count) {
    ereport(ERROR,
            (errmsg("Invalid source id %s:%d", tsfFdwOptions->filename, tsfFdwOptions->sourceId),
             errhint("There are %d sources in the TSF", tsf->source_count)));
  }
  tsf_source *tsfSource = &tsf->sources[tsfFdwOptions->sourceId - 1];

  /*
   * We use PgFdwRelationInfo to pass various information to subsequent
   * functions.
   */
  TsfFdwRelationInfo *fpinfo = (TsfFdwRelationInfo *)palloc0(sizeof(TsfFdwRelationInfo));
  baserel->fdw_private = (void *)fpinfo;
  fpinfo->columnList = columnList(baserel);  // Extract used fields only
  fpinfo->width = list_length(fpinfo->columnList);

  // matrix should be entity_count * locus_count
  switch (tsfFdwOptions->fieldType) {
    case FieldEntityAttribute:
      fpinfo->row_count = tsfSource->entity_count;
      fpinfo->rows_with_param_id = fpinfo->row_count;
      break;
    case FieldLocusAttribute:
      fpinfo->row_count = tsfSource->locus_count;
      fpinfo->rows_with_param_id = fpinfo->row_count;
      break;
    case FieldMatrix:
      fpinfo->row_count = tsfSource->locus_count * tsfSource->entity_count;
      fpinfo->rows_with_param_id = tsfSource->entity_count;
      fpinfo->is_matrix_source = true;
      fpinfo->rows_with_param_entity_id = tsfSource->locus_count;
      break;
    default: {
      ereport(
          ERROR,
          (errmsg("Invalid field type specified %s:%d", tsfFdwOptions->filename,
                  tsfFdwOptions->sourceId),
           errhint("The fieldtype option to the tsf_fdw tables must be set to 'l', 'm', or 'e'")));
    }
  }

  List *rowClauseList = baserel->baserestrictinfo;
  double rowSelectivity = clauselist_selectivity(root, rowClauseList, 0, JOIN_INNER, NULL);
  fpinfo->rows_selected = clamp_row_est(fpinfo->row_count * rowSelectivity);
  baserel->rows = fpinfo->rows_selected;

  // elog(INFO, "%s[%d][%d], rowCount %d, rowSelectivity: %f, clamped: %f", __func__,
  // tsfFdwOptions->sourceId, tsfFdwOptions->fieldType, fpinfo->row_count, rowSelectivity,
  // fpinfo->rows_selected);
}

static bool exprVarNameMatch(Expr *expr, Oid foreignTableId, PlannerInfo *root, const char *name)
{
  if (nodeTag(expr) == T_Var) {
    Var *var = (Var *)expr;
    if (var->varlevelsup == 0) {
      RangeTblEntry *rte = planner_rt_fetch(var->varno, root);
      if (rte->relid == foreignTableId) {
        char *columnName = get_attname(rte->relid, var->varattno);
        if (columnName && stricmp(columnName, name) == 0)
          return true;
      }
    }
  }
  return false;
}

/*
 * TsfGetForeignPaths creates possible access paths for a scan on the foreign
 * table.
 *
 * There is always a "default" full scan path, but given path_keys (ORDER
 * BY), filters and joins there may be other paths we can directly
 * internalize.
 */
static void TsfGetForeignPaths(PlannerInfo *root, RelOptInfo *baserel, Oid foreignTableId)
{
  TsfFdwRelationInfo *fpinfo = (TsfFdwRelationInfo *)baserel->fdw_private;

  /*
   * We skip reading columns that are not in query. Here we assume that all
   * columns in relation have the same width.
   */
  Cost startupCost = 10;
  Cost totalCost = (fpinfo->rows_selected * fpinfo->width) + startupCost;

  /*
   * Create simplest ForeignScan path node and add it to baserel.  This
   * path corresponds to SeqScan path of the table.  We already did all
   * the work to estimate cost and size of this path in
   * TsfGetForeignRelSize.
   */
  ForeignPath *path;
  path = create_foreignscan_path(root, baserel, baserel->rows, startupCost, totalCost,
                                 NIL,  /* no pathkeys */
                                 NULL, /* not parameterized */
                                 NIL); /* no fdw_private */

  add_path(baserel, (Path *)path);

  /*
   * Look for pathkeys that match the natural sort order of TSF tables
   */
  List *usable_pathkeys = NIL;
  ListCell *lc;
  foreach (lc, root->query_pathkeys) {
    PathKey *pathkey = (PathKey *)lfirst(lc);
    EquivalenceClass *pathkey_ec = pathkey->pk_eclass;
    Expr *em_expr;
    /*
     * Extract the expression and allow only the following pathkeys
     *  _id ASC
     *  (_id ASC, _entity_id ASC)
     */
    if (!pathkey_ec->ec_has_volatile && (em_expr = find_em_expr_for_rel(pathkey_ec, baserel)) &&
        pathkey->pk_strategy == BTLessStrategyNumber &&
        (exprVarNameMatch(em_expr, foreignTableId, root, "_id") ||
         (list_length(usable_pathkeys) == 1 &&
          exprVarNameMatch(em_expr, foreignTableId, root, "_entity_id"))))
      usable_pathkeys = lappend(usable_pathkeys, pathkey);
    else {
      /*
       * Any other pathkeys are not internalizable, so reset.
       */
      // elog(INFO, "[%f] Found NON usable pathkeys %s [%s]", baserel->rows,
      // nodeToString(pathkey_ec), nodeToString(usable_pathkeys));
      list_free(usable_pathkeys);
      usable_pathkeys = NIL;
      break;
    }
  }

  /* Create a path with useful pathkeys, if we found one. */
  if (usable_pathkeys != NULL) {
    // No different cost for including these pathkeys, since we are
    // inherently sorted by _id, _entity_id
    // elog(INFO, "[%f] Found %d usable pathkeys", baserel->rows, list_length(usable_pathkeys));
    add_path(baserel, (Path *)create_foreignscan_path(root, baserel, baserel->rows, startupCost,
                                                      totalCost, usable_pathkeys, NULL, NIL));
  }

  /*
   * Thumb through all join clauses for the rel to identify which outer
   * relations could supply one or more safe-to-handle join clauses.
   * We'll build a parameterized path for each such outer relation.
   *
   * It's convenient to manage this by representing each candidate outer
   * relation by the ParamPathInfo node for it.  We can then use the
   * ppi_clauses list in the ParamPathInfo node directly as a list of the
   * interesting join clauses for that rel.  This takes care of the
   * possibility that there are multiple safe join clauses for such a rel,
   * and also ensures that we account for unsafe join clauses that we'll
   * still have to enforce locally (since the parameterized-path machinery
   * insists that we handle all movable clauses).
   */
  List *ppi_list = NIL;
  foreach (lc, baserel->joininfo) {
    RestrictInfo *rinfo = (RestrictInfo *)lfirst(lc);
    Relids required_outer;
    ParamPathInfo *param_info;

    /* Check if clause can be moved to this rel */
    if (!join_clause_is_movable_to(rinfo, baserel))
      continue;

    /* The only paramaterizable expressions are:
     * _id = $
     * _entity_id = $
     */
    bool isEntityIdRestriction = false;
    if (!isParamatizable(foreignTableId, baserel, rinfo->clause, &isEntityIdRestriction))
      continue;

    /* Calculate required outer rels for the resulting path */
    required_outer = bms_union(rinfo->clause_relids, baserel->lateral_relids);
    /* We do not want the foreign rel itself listed in required_outer */
    required_outer = bms_del_member(required_outer, baserel->relid);

    /*
     * required_outer probably can't be empty here, but if it were, we
     * couldn't make a parameterized path.
     */
    if (bms_is_empty(required_outer))
      continue;

    /* Get the ParamPathInfo */
    param_info = get_baserel_parampathinfo(root, baserel, required_outer);
    Assert(param_info != NULL);

    /*
     * Add it to list unless we already have it.  Testing pointer equality
     * is OK since get_baserel_parampathinfo won't make duplicates.
     */
    int prev_length = list_length(ppi_list);
    ppi_list = list_append_unique_ptr(ppi_list, param_info);
    if (list_length(ppi_list) > prev_length) {
      double rows_selected = 1;  // for _id on non-matrix
      if (fpinfo->is_matrix_source) {
        if (isEntityIdRestriction)
          rows_selected = fpinfo->rows_with_param_entity_id;
        else
          rows_selected = fpinfo->rows_with_param_id;
      }

      totalCost = (rows_selected * fpinfo->width) + startupCost;
      add_path(baserel,
               (Path *)create_foreignscan_path(root, baserel, rows_selected, startupCost, totalCost,
                                               NIL,                       /* no pathkeys */
                                               param_info->ppi_req_outer, /* paramaterized path */
                                               NIL)                       /* no fdw_private list */
               );
    }
  }

  /*
   * The above scan examined only "generic" join clauses, not those that
   * were absorbed into EquivalenceClauses.  See if we can make anything
   * out of EquivalenceClauses.
   */
  if (baserel->has_eclass_joins) {
    /*
     * We repeatedly scan the eclass list looking for column references
     * (or expressions) belonging to the foreign rel.  Each time we find
     * one, we generate a list of equivalence joinclauses for it, and
     * then see if any are safe to send to the remote.  Repeat till there
     * are no more candidate EC members.
     */
    ec_member_foreign_arg arg;

    arg.already_used = NIL;
    for (;;) {
      List *clauses;

      /* Make clauses, skipping any that join to lateral_referencers */
      arg.current = NULL;
      clauses = generate_implied_equalities_for_column(root, baserel, ec_member_matches_foreign,
                                                       (void *)&arg, baserel->lateral_referencers);

      /* Done if there are no more expressions in the foreign rel */
      if (arg.current == NULL) {
        Assert(clauses == NIL);
        break;
      }

      ListCell *lc;

      /* Scan the extracted join clauses */
      foreach (lc, clauses) {
        RestrictInfo *rinfo = (RestrictInfo *)lfirst(lc);
        Relids required_outer;
        ParamPathInfo *param_info;

        /* Check if clause can be moved to this rel */
        if (!join_clause_is_movable_to(rinfo, baserel))
          continue;

        /* The only paramaterizable expressions are:
         * _id = $
         * _entity_id = $
         */
        // elog(INFO, "join clause: %s", nodeToString(rinfo->clause));
        bool isEntityIdRestriction = false;
        if (!isParamatizable(foreignTableId, baserel, rinfo->clause, &isEntityIdRestriction))
          continue;

        /* Calculate required outer rels for the resulting path */
        required_outer = bms_union(rinfo->clause_relids, baserel->lateral_relids);
        required_outer = bms_del_member(required_outer, baserel->relid);
        if (bms_is_empty(required_outer))
          continue;

        /* Get the ParamPathInfo */
        param_info = get_baserel_parampathinfo(root, baserel, required_outer);
        Assert(param_info != NULL);

        /* Add it to list unless we already have it */
        int prev_length = list_length(ppi_list);
        ppi_list = list_append_unique_ptr(ppi_list, param_info);
        if (list_length(ppi_list) > prev_length) {
          double rows_selected = 1;  // for _id on non-matrix
          if (fpinfo->is_matrix_source) {
            if (isEntityIdRestriction)
              rows_selected = fpinfo->rows_with_param_entity_id;
            else
              rows_selected = fpinfo->rows_with_param_id;
          }

          totalCost = (rows_selected * fpinfo->width) + startupCost;
          add_path(baserel,
                   (Path *)create_foreignscan_path(
                       root, baserel, rows_selected, startupCost, totalCost, NIL, /* no pathkeys */
                       param_info->ppi_req_outer, /* paramaterized path */
                       NIL)                       /* no fdw_private list */
                   );
        }
      }

      /* Try again, now ignoring the expression we found this time */
      arg.already_used = lappend(arg.already_used, arg.current);
    }
  }
}

/*
 * TsfGetForeignPlan creates a foreign scan plan node for scanning the
 * TSF table. We also add the query column list to scan nodes private
 * list, because we need it later for skipping over unused columns in the
 * query.
 */
static ForeignScan *TsfGetForeignPlan(PlannerInfo *root, RelOptInfo *baserel, Oid foreignTableId,
                                      ForeignPath *bestPath, List *targetList, List *scanClauses)
{
  TsfFdwRelationInfo *fpinfo = (TsfFdwRelationInfo *)baserel->fdw_private;
  Index scanRangeTableIndex = baserel->relid;
  List *localExprs = NIL;
  List *tsfExprs = NIL;
  Expr *entityIdClause = NULL;

  /*
   * Separate the scan_clauses into those that can be executed
   * internally and those that can't.
   */
  ListCell *lc = NULL;
  bool foundIdRestriction = false;
  foreach (lc, scanClauses) {
    RestrictInfo *rinfo = (RestrictInfo *)lfirst(lc);

    Assert(IsA(rinfo, RestrictInfo));

    /* Ignore any pseudoconstants, they're dealt with elsewhere */
    if (rinfo->pseudoconstant)
      continue;

    List *quals = NIL;
    extractRestrictions(baserel->relids, rinfo->clause, &quals);

    if (list_length(quals) == 1) {
      MulticornBaseQual *qual = (MulticornBaseQual *)linitial(quals);
      // ID restrictions must be only internalized expressions if they
      // are found. Otherwise we can have any number of internalizable
      // restriction expressions.
      if (!tsfExprs && isIdRestriction(foreignTableId, qual)) {
        tsfExprs = lappend(tsfExprs, rinfo->clause);
        foundIdRestriction = true;
      } else if (isEntityIdRestriction(foreignTableId, qual)) {
        entityIdClause = rinfo->clause;  // Compatible with ID restrictions, so don't set tsfExprs
      } else if (!foundIdRestriction && isInternableRestriction(foreignTableId, qual)) {
        tsfExprs = lappend(tsfExprs, rinfo->clause);
      } else {
        localExprs = lappend(localExprs, rinfo->clause);  // We don't hanle
      }
    } else {
      // Wasn't able to extract these restrictions...
      localExprs = lappend(localExprs, rinfo->clause);
    }
  }
  if (entityIdClause)
    tsfExprs = lappend(tsfExprs, entityIdClause);

  // elog(INFO, "[%f] extracted %d TSF, %d local quals", baserel->rows, list_length(tsfExprs),
  // list_length(localExprs));
  /*
   * As an optimization, we only read columns that are present in the
   * query.  We already extracted those columns and placed them in
   * fpinfo. We don't have access to baserel in executor's callback
   * functions, so we get the column list here and put it into foreign
   * scan node's private list, which gets copied over to the executor's
   * memory context.
   */
  List *foreignPrivateList = NIL;
  foreignPrivateList = list_make1(fpinfo->columnList);

  /* create the foreign scan node */
  ForeignScan *foreignScan = NULL;
  foreignScan = make_foreignscan(targetList,
                                 localExprs, /* postgres will run these restrictions on results */
                                 scanRangeTableIndex,
                                 tsfExprs,
                                 foreignPrivateList);
  return foreignScan;
}

/*
 * TsfExplainForeignScan produces extra output for the Explain command.
 */
static void TsfExplainForeignScan(ForeignScanState *scanState, ExplainState *explainState)
{
  Oid foreignTableId = RelationGetRelid(scanState->ss.ss_currentRelation);
  TsfFdwOptions *tsfFdwOptions = getTsfFdwOptions(foreignTableId);

  ExplainPropertyText("TSF File", tsfFdwOptions->filename, explainState);
  if (tsfFdwOptions->path)
    ExplainPropertyText("TSF Path", tsfFdwOptions->path, explainState);
  ExplainPropertyLong("TSF Source ID", tsfFdwOptions->sourceId, explainState);
  ExplainPropertyText("TSF Field Type", fieldTypeLetter(tsfFdwOptions->fieldType), explainState);

  /* supress file size if we're not showing cost details */
  if (explainState->costs) {
    struct stat statBuffer;

    int statResult = stat(tsfFdwOptions->filename, &statBuffer);
    if (statResult == 0) {
      ExplainPropertyLong("TSF File Size", (long)statBuffer.st_size, explainState);
    }
  }
}

/*
 * TsfBeginForeignScan opens the TSF file. Does not validate the query
 * yet, that happens of the first iteration.
 */
static void TsfBeginForeignScan(ForeignScanState *scanState, int executorFlags)
{
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
  TsfFdwOptions *tsfFdwOptions = getTsfFdwOptions(foreignTableId);

  /* create column mapping */
  ForeignScan *foreignScan = (ForeignScan *)scanState->ss.ps.plan;

  List *foreignPrivateList = foreignScan->fdw_private;
  Assert(list_length(foreignPrivateList) == 1);

  List *columnList = (List *)linitial(foreignPrivateList);

  /* create and set foreign execution state */
  TsfFdwExecState *executionState = (TsfFdwExecState *)palloc0(sizeof(TsfFdwExecState));
  executionState->fieldType = tsfFdwOptions->fieldType;
  /* opens sources for utalized columns */
  buildColumnMapping(foreignTableId, columnList, tsfFdwOptions, executionState);

  /* We extract the qual list, but execute it on first iteration through ForeingScan */
  List *foreignExprs = foreignScan->fdw_exprs;
  ListCell *lc = NULL;
  foreach (lc, foreignExprs) {
    extractRestrictions(bms_make_singleton(foreignScan->scan.scanrelid),
                        ((Expr *)lfirst(lc)),
                        &executionState->qualList);
  }

  // elog(INFO, "%s[%d][%d], cols %d quals %d", __func__, executionState->sourceId,
  // tsfFdwOptions->fieldType, list_length(columnList), list_length(foreignExprs));

  scanState->fdw_state = (void *)executionState;
}

/*
 * TsfIterateForeignScan reads the next record from TSF, converts it to
 * a PostgreSQL tuple, and stores the converted tuple into the ScanTupleSlot as
 * a virtual tuple.
 */
static TupleTableSlot *TsfIterateForeignScan(ForeignScanState *scanState)
{
  TsfFdwExecState *state = (TsfFdwExecState *)scanState->fdw_state;

  TupleTableSlot *tupleSlot = scanState->ss.ss_ScanTupleSlot;
  TupleDesc tupleDescriptor = tupleSlot->tts_tupleDescriptor;
  Datum *columnValues = tupleSlot->tts_values;
  bool *columnNulls = tupleSlot->tts_isnull;
  int32 columnCount = tupleDescriptor->natts;

  /*
   * We call in order ExecClearTuple, fillTupleSlot and ExecStoreVirtualTuple.
   *
   * If our iter is done, we just return an empty slot as required (not
   * filled).
   */
  ExecClearTuple(tupleSlot);

  if (!state->iter) {
    // Should only need to be done once, as we fill we clear cells
    memset(columnValues, 0, columnCount * sizeof(Datum));
    memset(columnNulls, true, columnCount * sizeof(bool));

    // Evalue expressions we extracted in qualList. This updates
    // state->entityIdList and state->idList.
    executeQualList(scanState, 0);

    // Initiate our iterators (one for each column and the master state->iter)
    initQuery(state);
  }

  if (state->idList) {
    // elog(INFO, "[%d][%d] id scan: %d", state->sourceId, state->fieldType, state->idListIdx);
    // Special iteration if we are doing ID lookups
    if (state->iter->cur_entity_idx >= 0 &&
        state->iter->cur_entity_idx + 1 < state->iter->entity_count) {
      // If there are more entities to scan for the current ID, use iter_next to scan those
      if (tsf_iter_next(state->iter)) {
        fillTupleSlot(state, columnValues, columnNulls);
        ExecStoreVirtualTuple(tupleSlot);
      }
    } else {
      state->idListIdx++;
      if (state->idListIdx < state->idListCount) {
        if (tsf_iter_id(state->iter, state->idList[state->idListIdx])) {
          fillTupleSlot(state, columnValues, columnNulls);
          ExecStoreVirtualTuple(tupleSlot);
        }
      }
    }
  } else {
    // Debug output first and last iteration to see if tables are doing full table scans
    // if(state->iter->cur_record_id < 0 || state->iter->cur_record_id >= state->iter->max_record_id
    // - 1) elog(INFO, "[%d][%d] iterscan: %d [%d of %d]", state->sourceId, state->fieldType,
    // state->iter->cur_record_id, state->iter->cur_entity_idx, state->iter->entity_count);

    if (iterateWithRestrictions(state)) {
      fillTupleSlot(state, columnValues, columnNulls);
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
static void TsfEndForeignScan(ForeignScanState *scanState)
{
  TsfFdwExecState *state = (TsfFdwExecState *)scanState->fdw_state;

  /* if we executed a query, reclaim tsf related resources */
  if (state != NULL) {
    // elog(INFO, "entering function %s[%d][%d] %p %p", __func__, state->sourceId, state->fieldType,
    // state, state->iter);
    tsf_iter_close(state->iter);

    for (int i = 0; i < state->columnCount; i++) {
      ColumnMapping *col = &state->columnMapping[i];
      tsf_iter_close(col->iter);
    }
    for (int i = 0; i < state->sourceCount; i++) {
      tsf_iter_close(state->sources[i].mappingIter);
    }
    free(state->idList);
    free(state->entityIdList);
  }
}

/*
 * TsfReScanForeignScan rescans the foreign table.
 *
 * Note: from tracing, it looks like TsfBeginForeignScan is always called before this.
 */
static void TsfReScanForeignScan(ForeignScanState *scanState)
{
  TsfFdwExecState *state = (TsfFdwExecState *)scanState->fdw_state;
  // elog(INFO, "entering function %s[%d][%d] isChanged: %d, %p %p", __func__, state->sourceId,
  // (bool)scanState->ss.ps.chgParam , state->fieldType, state, state->iter);
  if (state->iter) {
    /*
     * If any internal parameters affecting this node have changed, we
     * just re-evalue and update iterator instead of destorying it.
     */
    if (scanState->ss.ps.chgParam != NULL) {
      bool updatedEntityIds = false;
      executeQualList(scanState, &updatedEntityIds);
      state->iter->cur_entity_idx = -1;  // Reset inner entity counter
      if (updatedEntityIds) {
        // Reset iter and let it be re-opened in TsfIterateForeignScan
        // with the updated entityIds as input params.
        // elog(INFO, "reset entity id list");
        resetQuery(state);
        tsf_iter_close(state->iter);
        state->iter = NULL;
      }
    } else {
      // Close and clear iterator, TsfIterateForeignScan will rebuild
      resetQuery(state);
      tsf_iter_close(state->iter);
      state->iter = NULL;
    }
  }
}

/*
 * getOptionValue walks over foreign table and foreign server options, and
 * looks for the option with the given name. If found, the function returns the
 * option's value.
 */
static char *getOptionValue(Oid foreignTableId, const char *optionName)
{
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
 * TsfGetOptions returns the option values to be used when connecting to
 * and querying TSF. To resolve these values, the function checks the
 * foreign table's options.
 */
static TsfFdwOptions *getTsfFdwOptions(Oid foreignTableId)
{
  TsfFdwOptions *tsfFdwOptions = NULL;
  char *filename = NULL;
  char *path = NULL;
  char *fieldTypeStr = NULL;
  char *sourceIdName = NULL;
  int32 sourceId = 0;

  filename = getOptionValue(foreignTableId, OPTION_NAME_FILENAME);
  path = getOptionValue(foreignTableId, OPTION_NAME_PATH);
  sourceIdName = getOptionValue(foreignTableId, OPTION_NAME_SOURCEID);
  sourceId = pg_atoi(sourceIdName, sizeof(int32), 1);
  fieldTypeStr = getOptionValue(foreignTableId, OPTION_NAME_FIELDTYPE);

  tsfFdwOptions = (TsfFdwOptions *)palloc0(sizeof(TsfFdwOptions));
  tsfFdwOptions->filename = filename;
  tsfFdwOptions->path = path;
  tsfFdwOptions->sourceId = sourceId;
  tsfFdwOptions->fieldType = FieldTypeInvalid;
  if (fieldTypeStr) {
    if (strncmp(fieldTypeStr, "l", NAMEDATALEN) == 0)
      tsfFdwOptions->fieldType = FieldLocusAttribute;
    if (strncmp(fieldTypeStr, "e", NAMEDATALEN) == 0)
      tsfFdwOptions->fieldType = FieldEntityAttribute;
    if (strncmp(fieldTypeStr, "m", NAMEDATALEN) == 0)
      tsfFdwOptions->fieldType = FieldMatrix;
  }

  return tsfFdwOptions;
}

/*
 * ColumnList takes in the planner's information about this foreign table. The
 * function then finds all columns needed for query execution, including those
 * used in projections, joins, and filter clauses, de-duplicates these columns,
 * and returns them in a new list. This function is unchanged from mongo_fdw.
 */
static List *columnList(RelOptInfo *baserel)
{
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
    clauseColumnList =
        pull_var_clause(restrictClause, PVC_RECURSE_AGGREGATES, PVC_RECURSE_PLACEHOLDERS);

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

/* Join two strings into a new string using palloc */
static char *strJoin(const char *left, const char *right, char joiner)
{
  bool use_joiner = joiner != '\0';
  int left_size = left ? strlen(left) : 0;
  int size = left_size + (use_joiner ? 1 : 0) + strlen(right) + 1;
  char *joined = palloc0(size);
  if (left) {
    memcpy(joined, left, left_size);
  }
  int offset = left_size;
  if (use_joiner)
    joined[offset++] = joiner;
  memcpy(joined + offset, right, strlen(right) + 1);
  return joined;
}

/*
 * buildColumnMapping creates a array of ColumnMapping objects the length
 * of observed column variables (in columnList). Each mapping has the
 * index into the values/nulls array it should fill the tuple (columnId)
 * as well as linkage to its TSF soruce, field and mapping source.
 */
static void buildColumnMapping(Oid foreignTableId, List *columnList, TsfFdwOptions *tsfFdwOptions,
                               TsfFdwExecState *executionState)
{
  ListCell *columnCell = NULL;
  int length = list_length(columnList);
  executionState->columnMapping = palloc0(sizeof(ColumnMapping) * length);
  executionState->columnCount = 0;
  executionState->sourceCount = 0;
  executionState->idColumnIndex = -1;
  executionState->entityIdColumnIndex = -1;

  foreach (columnCell, columnList) {
    char *filename = tsfFdwOptions->filename;
    int sourceId = tsfFdwOptions->sourceId;
    int mappingId = -1;
    int fieldIdx = -1;
    Var *column = (Var *)lfirst(columnCell);
    AttrNumber columnId = column->varattno;

    // Check for our sentinal _id, _entity_id fields not in backend tables
    char *columnName = get_relid_attribute_name(foreignTableId, columnId);
    if (stricmp(columnName, "_id") == 0) {
      executionState->idColumnIndex = columnId - 1;
      continue;
    }
    if (stricmp(columnName, "_entity_id") == 0) {
      executionState->entityIdColumnIndex = columnId - 1;
      continue;
    }

    // Check for our per-field options that override table-level options
    ListCell *lc;
    List *options = GetForeignColumnOptions(foreignTableId, columnId);
    foreach (lc, options) {
      DefElem *def = (DefElem *)lfirst(lc);
      if (strcmp(def->defname, "filename") == 0) {
        filename = defGetString(def);
      }
      if (strcmp(def->defname, "sourceid") == 0) {
        sourceId = strtod(defGetString(def), NULL);
      }
      if (strcmp(def->defname, "mappingid") == 0) {
        mappingId = strtod(defGetString(def), NULL);
      }
      if (strcmp(def->defname, "fieldidx") == 0) {
        fieldIdx = strtod(defGetString(def), NULL);
      }
    }

    // Find tsfSouce
    ColumnMapping *columnMapping = &executionState->columnMapping[executionState->columnCount];
    executionState->columnCount++;

    const char *tsfFileName = strJoin(tsfFdwOptions->path, filename, '\0');  // use base path
    columnMapping->sourceIdx = getTsfSource(tsfFileName, sourceId, executionState);
    TsfSourceState *tsfSourceState = &executionState->sources[columnMapping->sourceIdx];
    tsf_source *tsfSource = &tsfSourceState->tsf->sources[tsfSourceState->sourceId - 1];

    columnMapping->mappingSourceIdx = -1;
    if (mappingId >= 0)
      columnMapping->mappingSourceIdx = getTsfSource(tsfFileName, mappingId, executionState);

    // Find this coloumn by its symbol name in the source if no fieldIdx provided
    if (fieldIdx < 0) {
      for (int j = 0; j < tsfSource->field_count; j++) {
        if (stricmp(tsfSource->fields[j].symbol, columnName) == 0) {
          fieldIdx = j;
          break;
        }
      }
    } else {
      // valid fieldIdx
      if (fieldIdx >= tsfSource->field_count) {
        ereport(ERROR,
                (errmsg("Provided fieldidx column option out of bounds"),
                 errhint("Source for field %s has %d fields", columnName, tsfSource->field_count)));
      }
    }
    columnMapping->fieldIdx = fieldIdx;

    if (columnMapping->fieldIdx < 0) {
      ereport(ERROR, (errmsg("Cound not find referenced field in TSF table"),
                      errhint("Field in query: %s", columnName)));
    }

    columnMapping->columnIndex = columnId - 1;
    columnMapping->columnTypeId = column->vartype;
    columnMapping->columnArrayTypeId = get_element_type(column->vartype);
  }

  if (executionState->sourceCount == 0) {
    // No source-based fields, oepn default file to drive iteration
    getTsfSource(strJoin(tsfFdwOptions->path, tsfFdwOptions->filename, '\0'),
                 tsfFdwOptions->sourceId, executionState);
  }
}

static bool isIdRestriction(Oid foreignTableId, MulticornBaseQual *qual)
{
  char *columnName = get_relid_attribute_name(foreignTableId, qual->varattno);

  if (stricmp(columnName, "_id") == 0 && strcmp(qual->opname, "=") == 0) {
    // Scalar or useOr
    return !qual->isArray || qual->useOr;
  }
  return false;
}

static bool isEntityIdRestriction(Oid foreignTableId, MulticornBaseQual *qual)
{
  char *columnName = get_relid_attribute_name(foreignTableId, qual->varattno);

  if (stricmp(columnName, "_entity_id") == 0 && strcmp(qual->opname, "=") == 0) {
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
  return false;  // Not yet implemented
}

/*
 * Returns true if given expr can be cheaply paramaterized (i.e. is _id = $ or _entity_id = $)
 */
static bool isParamatizable(Oid foreignTableId, RelOptInfo *baserel, Expr *expr,
                            bool *outIsEntityIdRestriction)
{
  List *quals = NIL;
  extractRestrictions(baserel->relids, expr, &quals);

  if (list_length(quals) == 1) {
    MulticornBaseQual *qual = (MulticornBaseQual *)linitial(quals);
    // Only pamaterizing ID restrictions
    if (isIdRestriction(foreignTableId, qual))
      return true;
    if (isEntityIdRestriction(foreignTableId, qual)) {
      *outIsEntityIdRestriction = true;
      return true;
    }
  }
  return false;
}

static void parseQualIntoIdList(int **idList, int *idListCount, bool isNull, bool isArray,
                                Datum value)
{
  if (isNull) {
    free(*idList);
    *idList = calloc(sizeof(int), 1);  // non used, but must be not-null
    *idListCount = 0;
  } else if (!isNull && isArray) {
    ArrayType *array = DatumGetArrayTypeP(value);
    Oid elmtype = ARR_ELEMTYPE(array);
    Datum *dvalues;
    bool *dnulls;
    int nelems;
    int16 elmlen;
    bool elmbyval;
    char elmalign;
    get_typlenbyvalalign(elmtype, &elmlen, &elmbyval, &elmalign);
    deconstruct_array(array, elmtype, elmlen, elmbyval, elmalign, &dvalues, &dnulls, &nelems);
    if (*idListCount != nelems) {
      free(*idList);
      *idList = calloc(sizeof(int), nelems);
    }
    *idListCount = 0;
    for (int i = 0; i < nelems; i++) {
      if (!dnulls[i]) {
        (*idList)[*idListCount] = DatumGetInt32(dvalues[i]);
        (*idListCount) += 1;
      }
    }
  } else {
    if (*idListCount != 1) {
      free(*idList);
      *idList = calloc(sizeof(int), 1);
      *idListCount = 1;
    }
    // assert type?
    *idList[0] = DatumGetInt32(value);
  }
}

static void executeQualList(ForeignScanState *scanState, bool *updatedEntityIds)
{
  TsfFdwExecState *state = (TsfFdwExecState *)scanState->fdw_state;
  ListCell *lc;

  ExprContext *econtext = scanState->ss.ps.ps_ExprContext;

  foreach (lc, state->qualList) {
    MulticornBaseQual *qual = lfirst(lc);
    bool isNull;
    Datum value;
    ExprState *expr_state = NULL;
    bool usable = false;

    switch (qual->right_type) {
      case T_Param:
        usable = true;
        expr_state = ExecInitExpr(((MulticornParamQual *)qual)->expr, (PlanState *)scanState);
        value = ExecEvalExpr(expr_state, econtext, &isNull, NULL);
        break;
      case T_Const:
        usable = true;
        value = ((MulticornConstQual *)qual)->value;
        isNull = ((MulticornConstQual *)qual)->isnull;
        break;
      default:
        break;
    }
    if (usable) {
      // Process const qual into TSF based query state manager.
      // Special case for _id and _entity_id
      if (state->idColumnIndex >= 0 && qual->varattno - 1 == state->idColumnIndex) {
        // _id qual
        state->idListIdx = -1;
        parseQualIntoIdList(&state->idList, &state->idListCount, isNull, qual->isArray, value);
      }
      if (state->entityIdColumnIndex >= 0 && qual->varattno - 1 == state->entityIdColumnIndex) {
        // _entity_id qual
        parseQualIntoIdList(&state->entityIdList, &state->entityIdListCount, isNull, qual->isArray,
                            value);
        if (updatedEntityIds)
          *updatedEntityIds = true;
      }
    }
  }
}

static void initQuery(TsfFdwExecState *state)
{
  for (int i = 0; i < state->columnCount; i++) {
    ColumnMapping *col = &state->columnMapping[i];
    Assert(col->sourceIdx >= 0 && col->sourceIdx < state->sourceCount);
    TsfSourceState *source = &state->sources[col->sourceIdx];
    col->iter = tsf_query_table(source->tsf, source->sourceId, 1, &col->fieldIdx,
                                state->entityIdListCount, state->entityIdList, state->fieldType);
    if (!col->iter) {
      ereport(ERROR, (errmsg("Failed to start TSF field table query"),
                      errhint("Query failed on field %d with source %s:%d", col->columnIndex,
                              source->fileName, source->sourceId)));
    }
    if (col->mappingSourceIdx >= 0) {
      TsfSourceState *mapping = &state->sources[col->mappingSourceIdx];
      if (!mapping->mappingIter) {
        // Because a mapping source will be used by every field of the
        // source it is mapping to the primary table, and it only has a
        // single mapping field, we have the source own the mappingIter
        // and use it for all fields using mappingSourceIdx.
        int fields[1];
        fields[0] = 0;  // mapping field is always 0 in mapping source
        tsf_field_type mappingFieldType =
            state->fieldType == FieldEntityAttribute ? FieldEntityAttribute : FieldLocusAttribute;

        mapping->mappingIter =
            tsf_query_table(mapping->tsf, mapping->sourceId, 1, fields, -1, NULL, mappingFieldType);
        if (!mapping->mappingIter) {
          ereport(ERROR, (errmsg("Failed to start TSF mapping field table query"),
                          errhint("Query failed on mapping of field %d with source %s:%d",
                                  col->columnIndex, mapping->fileName, mapping->sourceId)));
        }
      }
      col->mappingIter = mapping->mappingIter;
    }
  }

  // Setup up a global iterator with no fields
  Assert(state->sourceCount > 0);
  TsfSourceState *source = &state->sources[0];
  state->iter = tsf_query_table(source->tsf, source->sourceId, 0, NULL, state->entityIdListCount,
                                state->entityIdList, state->fieldType);
  if (!state->iter) {
    ereport(ERROR, (errmsg("Failed to start TSF ID table query"),
                    errhint("Query failed on %d fields", state->columnCount)));
  }
}

static void resetQuery(TsfFdwExecState *state)
{
  for (int i = 0; i < state->columnCount; i++) {
    ColumnMapping *col = &state->columnMapping[i];
    tsf_iter_close(col->iter);
    col->iter = NULL;
    tsf_iter_close(col->mappingIter);
    col->mappingIter = NULL;
  }
  tsf_iter_close(state->iter);
  state->iter = NULL;
}

static bool iterateWithRestrictions(TsfFdwExecState *state)
{
  // Advanced our master iterator along until all of our restricitons are satisifed or it reaches
  // the end. Returns false when at end-of-table.
  while (tsf_iter_next(state->iter)) {
    bool allRestrictionsSatisifed = true;
    for (int i = 0; i < state->restrictionCount; i++) {
      // TODO: Build out this pseudocode
      // if mapping
      //  read state->restricitons[i]->col->mappingIter
      //  someEvaled = false
      //  for each in mapping:
      //   read value
      //   if eval conditional (all mapping is ANY(restriction) semantics)
      //     someEvaled = true
      //     break
      //  if not someEvalued:
      //    allRestrictionsSatisifed = false;
      //    break;
      // read state->restricitons[i]->col->iter
      //  if not eval conditional
      //    allRestrictionsSatisifed = false;
      //    break;
    }
    if (allRestrictionsSatisifed)
      return true;
    // Otherwise loop and eval next record
  }
  return false;  // End of table
}

/* Sync iter to ref_iter */
static void syncIter(tsf_iter *iter, tsf_iter *ref_iter)
{
  if (iter->is_matrix_iter)
    tsf_iter_id_matrix(iter, ref_iter->cur_record_id, ref_iter->cur_entity_idx);
  else
    tsf_iter_id(iter, ref_iter->cur_record_id);
}

/*
 * Does the heavy lifting of reading through each value of a tsf arrya
 * type and create an array of Datum*.
 */
static void columnValueArrayData(tsf_v value, tsf_field *f, Oid valueTypeId, int *outSize,
                                 Datum **outValueArray)
{
  int size = va_size(value);
  Datum *columnValueArray = palloc0(size * sizeof(Datum));
  bool typeMatched = false;
  switch (valueTypeId) {
    case INT4OID:
      if (f->value_type == TypeInt32Array) {
        typeMatched = true;
        for (int i = 0; i < size; i++)
          columnValueArray[i] = Int32GetDatum(va_int32(value, i));
      }
      break;
    case FLOAT4OID:
      if (f->value_type == TypeFloat32Array) {
        typeMatched = true;
        for (int i = 0; i < size; i++)
          columnValueArray[i] = Float4GetDatum(va_float32(value, i));
      }
      break;
    case FLOAT8OID:
      if (f->value_type == TypeFloat64Array) {
        typeMatched = true;
        for (int i = 0; i < size; i++)
          columnValueArray[i] = Float8GetDatum(va_float64(value, i));
      }
      break;
    case BOOLOID:
      if (f->value_type == TypeBoolArray) {
        typeMatched = true;
        for (int i = 0; i < size; i++)
          columnValueArray[i] = BoolGetDatum(va_bool(value, i));
      }
      break;
    case TEXTOID:
      if (f->value_type == TypeStringArray) {
        typeMatched = true;
        const char *s = va_array(value);
        for (int i = 0; i < size; i++) {
          columnValueArray[i] = CStringGetTextDatum(s);
          // Advanced past next NULL
          while (s[0] != '\0')  // increment to next NULL
            s++;
          s++;  // go past the NULL
        }
      } else if (f->value_type == TypeEnumArray) {
        typeMatched = true;
        for (int i = 0; i < size; i++) {
          const char *s = va_enum_as_str(value, i, f->enum_names);
          if (s)
            columnValueArray[i] = CStringGetTextDatum(s);
          else
            columnValueArray[i] = CStringGetTextDatum("");
        }
      }
      break;
    default:
      break;  // typeMatched will be false
  }
  if (!typeMatched) {
    ereport(ERROR,
            (errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
             errmsg("cannot convert tsf array type to column type"),
             errhint("Array value type: %u, Tsf type: %d", (uint32)valueTypeId, f->value_type)));
    return;  // NULL Datum
  }
  *outSize = size;
  *outValueArray = columnValueArray;
}

/*
 * columnValueArray uses array element type id to read the current array pointed
 * to by the tsf_v value, and converts each array element (with matching type)
 * to the corresponding PostgreSQL datum. Then, the function constructs an array
 * datum from element datums, and returns the array datum.
 */
static Datum columnValueArray(tsf_v value, tsf_field *f, Oid valueTypeId)
{
  int size;
  Datum *columnValueArray;
  columnValueArrayData(value, f, valueTypeId, &size, &columnValueArray);

  bool typeByValue = false;
  char typeAlignment = 0;
  int16 typeLength = 0;
  get_typlenbyvalalign(valueTypeId, &typeLength, &typeByValue, &typeAlignment);
  ArrayType *columnValueObject =
      construct_array(columnValueArray, size, valueTypeId, typeLength, typeByValue, typeAlignment);

  return PointerGetDatum(columnValueObject);
}

/*
 * columnValue uses reads data from the TSF value into a PostgreSQL datum
 * of the provided type.
 *
 * Does not handle case where TSF values are missing (check is_null
 * before calling this).
 *
 * NOTE: This function assumes the foriegn table is constructed with
 * appropriate types for each field (which it will be if the schema is
 * generated by this FDW).
 */
static Datum columnValue(tsf_v value, tsf_field *f, Oid columnTypeId)
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
      else if (f->value_type == TypeBool)
        str = v_bool(value) == 0 ? "False" : "True";
      if (str)
        return CStringGetTextDatum(str);
      break;
    }
    default: {
      break;  // Any fallthrough generates an error
    }
  }
  ereport(ERROR, (errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
                  errmsg("cannot convert tsf type to column type"),
                  errhint("Column type: %u, Tsf type: %d", (uint32)columnTypeId, f->value_type)));

  return 0;  // NULL Datum
}

/*
 * fillTupleSlot walks over the queried fields in execution state, for
 * each one it reads the TSF value, converts it to the appropriate
 * PostgreSQL Datum type and places it in columnValues
 */
static void fillTupleSlot(TsfFdwExecState *state, Datum *columnValues, bool *columnNulls)
{
  for (int i = 0; i < state->columnCount; i++) {
    ColumnMapping *col = &state->columnMapping[i];
    Oid columnTypeId = col->columnTypeId;
    Oid columnArrayTypeId = col->columnArrayTypeId;
    int32 columnIndex = col->columnIndex;
    tsf_field *f = col->iter->fields[0];

    // Clear and set NULL by default
    columnValues[columnIndex] = (Datum)0;
    columnNulls[columnIndex] = true;

    if (col->mappingIter) {
      // Look up from type table values need for construct_md_array
      bool typeByValue = false;
      char typeAlignment = 0;
      int16 typeLength = 0;
      if (OidIsValid(columnArrayTypeId))
        get_typlenbyvalalign(columnArrayTypeId, &typeLength, &typeByValue, &typeAlignment);

      syncIter(col->mappingIter, state->iter);
      tsf_field *mf = col->mappingIter->fields[0];
      if (mf->value_type == TypeInt32Array) {
        Assert(OidIsValid(columnArrayTypeId));  // field type must be array
        int size = va_size(col->mappingIter->cur_values[0]);
        if (size == 0)
          continue;  // Mapped fields also NULL when 0 elements mapped

        // Otherwise we will always place a array with first dim size in columnValues
        columnNulls[columnIndex] = false;

        // Build up array
        Datum *elements = palloc0(size * sizeof(Datum));
        int *sizes = palloc0(size * sizeof(int));
        int maxSubElementSize = 0;
        bool colIsArray = f->value_type == TypeInt32Array || f->value_type == TypeFloat32Array ||
                          f->value_type == TypeFloat64Array || f->value_type == TypeBoolArray ||
                          f->value_type == TypeStringArray || f->value_type == TypeEnumArray;

        // If colIsArray, then we build a two-dimentional postgres
        // array. This requires the second dimention to be a fixed size,
        // which will be null-padded based on maxSubElementSize.
        for (int i = 0; i < size; i++) {
          // Read the ID at 'i' in the mapping field
          if (col->iter->is_matrix_iter)
            tsf_iter_id_matrix(col->iter, va_int32(col->mappingIter->cur_values[0], i),
                               state->iter->cur_entity_idx);
          else
            tsf_iter_id(col->iter, va_int32(col->mappingIter->cur_values[0], i));
          tsf_v value = col->iter->cur_values[0];
          bool is_null = col->iter->cur_nulls[0];

          if (colIsArray) {
            int elementSize;
            Datum *columnValueArray;
            columnValueArrayData(value, f, columnArrayTypeId, &elementSize, &columnValueArray);
            if (elementSize > maxSubElementSize)
              maxSubElementSize = elementSize;
            elements[i] = PointerGetDatum(columnValueArray);
            sizes[i] = elementSize;
          } else {
            if (!is_null)
              elements[i] = columnValue(value, f, columnArrayTypeId);
          }
        }

        if (colIsArray) {
          if (maxSubElementSize == 0) {
            // Inner arrays are all empty, so make array of NULLs. {NULL,NULL,...}
            int dims[1];
            int lbs[1];
            dims[0] = size;
            lbs[0] = 1;
            bool *nulls = palloc0(size * sizeof(bool));
            for (int i = 0; i < size; i++)
              nulls[i] = true;
            ArrayType *array = construct_md_array(NULL, nulls, 1, dims, lbs, columnArrayTypeId,
                                                  typeLength, typeByValue, typeAlignment);
            columnValues[columnIndex] = PointerGetDatum(array);
            pfree(nulls);
          } else {
            // 2D array
            int dims[2];
            int lbs[2];
            dims[0] = size;
            dims[1] = maxSubElementSize;
            lbs[0] = 1;
            lbs[1] = 1;
            int nelems = size * maxSubElementSize;
            Datum *elems = palloc0(nelems * sizeof(Datum *));
            bool *nulls = palloc0(nelems * sizeof(bool));
            int k = 0;
            for (int i = 0; i < size; i++) {
              int elemSize = sizes[i];
              Datum *innerArray = (Datum *)DatumGetPointer(elements[i]);
              for (int j = 0; j < maxSubElementSize; j++) {
                if (j < elemSize)
                  elems[k] = innerArray[j];
                else
                  nulls[k] = true;
                k++;
              }
              pfree(innerArray);
            }
            ArrayType *array = construct_md_array(elems, nulls, 2, dims, lbs, columnArrayTypeId,
                                                  typeLength, typeByValue, typeAlignment);
            columnValues[columnIndex] = PointerGetDatum(array);
            pfree(nulls);
            pfree(elems);
          }
        } else {
          int dims[1];
          int lbs[1];
          dims[0] = size;
          lbs[0] = 1;
          bool *nulls = palloc0(size * sizeof(bool));
          for (int i = 0; i < size; i++)
            if (!elements[i])  // NULL Datum is our null sentinal
              nulls[i] = true;
          ArrayType *array = construct_md_array(elements, nulls, 1, dims, lbs, columnArrayTypeId,
                                                typeLength, typeByValue, typeAlignment);
          columnValues[columnIndex] = PointerGetDatum(array);
          pfree(nulls);
        }
        pfree(elements);
        pfree(sizes);
        continue;  // Done with 1:M mapping loading of columnValues
      } else {
        Assert(mf->value_type == TypeInt32);
        int id = v_int32(col->mappingIter->cur_values[0]);
        if (col->iter->is_matrix_iter)
          tsf_iter_id_matrix(col->iter, id, state->iter->cur_entity_idx);
        else
          tsf_iter_id(col->iter, id);
        // Note: explicit fall-through to code below that assumes col->iter is placed
      }
    } else {
      // For non-mapped, we need to read by ID for each field
      syncIter(col->iter, state->iter);
    }

    // iter has been updated by a mapping field or being driven directly
    tsf_v value = col->iter->cur_values[0];
    bool is_null = col->iter->cur_nulls[0];
    if (is_null)
      continue;  // default

    /* fill in corresponding column value and null flag */
    if (OidIsValid(columnArrayTypeId)) {
      if (va_size(value) == 0)
        continue;  // size=0 represented as NULL

      columnValues[columnIndex] = columnValueArray(value, f, columnArrayTypeId);
      columnNulls[columnIndex] = false;  // TODO: empty array will be NULL
    } else {
      columnValues[columnIndex] = columnValue(value, f, columnTypeId);
      columnNulls[columnIndex] = false;
    }
  }

  // Fill in ID columns
  if (state->idColumnIndex >= 0) {
    columnValues[state->idColumnIndex] = Int32GetDatum(state->iter->cur_record_id);
    columnNulls[state->idColumnIndex] = false;
  }
  if (state->entityIdColumnIndex >= 0) {
    columnValues[state->entityIdColumnIndex] =
        Int32GetDatum(state->iter->entity_ids[state->iter->cur_entity_idx]);
    columnNulls[state->entityIdColumnIndex] = false;
  }
}
