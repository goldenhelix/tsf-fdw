/*-------------------------------------------------------------------------
 *
 * query.c
 *
 * Ulities for parsing out qualifications from the query.
 *
 * Copyright (c) 2015 Golden Helix, Inc.
 *
 * Heavily based on the excellent Multicorn libary by Kozea (see LICENSE_Kozea)
 *
 * https://github.com/Kozea/Multicorn/
 *
 *-------------------------------------------------------------------------
 */
#include "query.h"

#include "optimizer/var.h"
#include "optimizer/clauses.h"
#include "optimizer/pathnode.h"
#include "optimizer/subselect.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_operator.h"
#include "mb/pg_wchar.h"
#include "utils/lsyscache.h"
#include "parser/parsetree.h"

bool extractClauseFromOpExpr(Relids base_relids, OpExpr *node, List **quals);

bool extractClauseFromNullTest(Relids base_relids, NullTest *node, List **quals);

bool extractClauseFromScalarArrayOpExpr(Relids base_relids, ScalarArrayOpExpr *node, List **quals);

bool extractClauseFromBoolVar(Relids base_relids, Var *node, List **quals, bool value);

bool extractClauseBoolVarFromBoolExpr(Relids base_relids, BoolExpr* op, List **quals);

bool extractClauseWithNullFromBoolExpr(Relids base_relids, BoolExpr *node, List **quals);

char *getOperatorString(Oid opoid);

MulticornBaseQual *makeQual(AttrNumber varattno, char *opname, Expr *value, bool isarray,
                            bool useOr, bool includeMissing);

Node *unnestClause(Node *node);
void swapOperandsAsNeeded(Node **left, Node **right, Oid *opoid, Relids base_relids);
OpExpr *canonicalOpExpr(OpExpr *opExpr, Relids base_relids);
ScalarArrayOpExpr *canonicalScalarArrayOpExpr(ScalarArrayOpExpr *opExpr, Relids base_relids);

bool isAttrInRestrictInfo(Index relid, AttrNumber attno, RestrictInfo *restrictinfo);

List *clausesInvolvingAttr(Index relid, AttrNumber attnum, EquivalenceClass *eq_class);

/*
 * The list of needed columns (represented by their respective vars)
 * is pulled from:
 *	- the targetcolumns
 *	- the restrictinfo
 */
List *extractColumns(List *reltargetlist, List *restrictinfolist)
{
  ListCell *lc;
  List *columns = NULL;
  int i = 0;

  foreach (lc, reltargetlist) {
    List *targetcolumns;
    Node *node = (Node *)lfirst(lc);

    targetcolumns = pull_var_clause(node, PVC_RECURSE_AGGREGATES, PVC_RECURSE_PLACEHOLDERS);
    columns = list_union(columns, targetcolumns);
    i++;
  }
  foreach (lc, restrictinfolist) {
    List *targetcolumns;
    RestrictInfo *node = (RestrictInfo *)lfirst(lc);

    targetcolumns =
        pull_var_clause((Node *)node->clause, PVC_RECURSE_AGGREGATES, PVC_RECURSE_PLACEHOLDERS);
    columns = list_union(columns, targetcolumns);
  }
  return columns;
}

/*
 * Initialize the array of "ConversionInfo" elements, needed to convert python
 * objects back to suitable postgresql data structures.
 */
void initConversioninfo(ConversionInfo **cinfos, AttInMetadata *attinmeta)
{
  int i;

  for (i = 0; i < attinmeta->tupdesc->natts; i++) {
    Form_pg_attribute attr = attinmeta->tupdesc->attrs[i];
    Oid outfuncoid;
    bool typIsVarlena;

    if (!attr->attisdropped) {
      ConversionInfo *cinfo = palloc0(sizeof(ConversionInfo));

      cinfo->attoutfunc = (FmgrInfo *)palloc0(sizeof(FmgrInfo));
      getTypeOutputInfo(attr->atttypid, &outfuncoid, &typIsVarlena);
      fmgr_info(outfuncoid, cinfo->attoutfunc);
      cinfo->atttypoid = attr->atttypid;
      cinfo->atttypmod = attinmeta->atttypmods[i];
      cinfo->attioparam = attinmeta->attioparams[i];
      cinfo->attinfunc = &attinmeta->attinfuncs[i];
      cinfo->attrname = NameStr(attr->attname);
      cinfo->attnum = i + 1;
      cinfo->attndims = attr->attndims;
      cinfo->need_quote = false;
      cinfos[i] = cinfo;
    } else {
      cinfos[i] = NULL;
    }
  }
}

char *getOperatorString(Oid opoid)
{
  HeapTuple tp;
  Form_pg_operator operator;

  tp = SearchSysCache1(OPEROID, ObjectIdGetDatum(opoid));
  if (!HeapTupleIsValid(tp))
    elog(ERROR, "cache lookup failed for operator %u", opoid);
  operator=(Form_pg_operator) GETSTRUCT(tp);
  ReleaseSysCache(tp);
  return NameStr(operator->oprname);
}

/*
 * Returns the node of interest from a node.
 */
Node *unnestClause(Node *node)
{
  switch (node->type) {
    case T_RelabelType:
      return (Node *)((RelabelType *)node)->arg;
    case T_ArrayCoerceExpr:
      return (Node *)((ArrayCoerceExpr *)node)->arg;
    default:
      return node;
  }
}

void swapOperandsAsNeeded(Node **left, Node **right, Oid *opoid, Relids base_relids)
{
  HeapTuple tp;
  Form_pg_operator op;
  Node *l = *left, *r = *right;

  tp = SearchSysCache1(OPEROID, ObjectIdGetDatum(*opoid));
  if (!HeapTupleIsValid(tp))
    elog(ERROR, "cache lookup failed for operator %u", *opoid);
  op = (Form_pg_operator)GETSTRUCT(tp);
  ReleaseSysCache(tp);
  /* Right is already a var. */
  /* If "left" is a Var from another rel, and right is a Var from the */
  /* target rel, swap them. */
  /* Same thing is left is not a var at all. */
  /* To swap them, we have to lookup the commutator operator. */
  if (IsA(r, Var)) {
    Var *rvar = (Var *)r;

    if (!IsA(l, Var) || (!bms_is_member(((Var *)l)->varno, base_relids) &&
                         bms_is_member(rvar->varno, base_relids))) {
      /* If the operator has no commutator operator, */
      /* bail out. */
      if (op->oprcom == 0) {
        return;
      }
      {
        *left = r;
        *right = l;
        *opoid = op->oprcom;
      }
    }
  }
}

/*
 * Swaps the operands if needed / possible, so that left is always a node
 * belonging to the baserel and right is either:
 *	- a Const
 *	- a Param
 *	- a Var from another relation
 */
OpExpr *canonicalOpExpr(OpExpr *opExpr, Relids base_relids)
{
  Oid operatorid = opExpr->opno;
  Node *l, *r;
  OpExpr *result = NULL;

  /* Only treat binary operators for now. */
  if (list_length(opExpr->args) == 2) {
    l = unnestClause(list_nth(opExpr->args, 0));
    r = unnestClause(list_nth(opExpr->args, 1));
    swapOperandsAsNeeded(&l, &r, &operatorid, base_relids);
    if (IsA(l, Var) && bms_is_member(((Var *)l)->varno, base_relids) && ((Var *)l)->varattno >= 1) {
      result = (OpExpr *)make_opclause(operatorid, opExpr->opresulttype, opExpr->opretset,
                                       (Expr *)l, (Expr *)r, opExpr->opcollid, opExpr->inputcollid);
    }
  }
  return result;
}

/*
 * Swaps the operands if needed / possible, so that left is always a node
 * belonging to the baserel and right is either:
 *	- a Const
 *	- a Param
 *	- a Var from another relation
 */
ScalarArrayOpExpr *canonicalScalarArrayOpExpr(ScalarArrayOpExpr *opExpr, Relids base_relids)
{
  Oid operatorid = opExpr->opno;
  Node *l, *r;
  ScalarArrayOpExpr *result = NULL;
  HeapTuple tp;
  Form_pg_operator op;

  /* Only treat binary operators for now. */
  if (list_length(opExpr->args) == 2) {
    l = unnestClause(list_nth(opExpr->args, 0));
    r = unnestClause(list_nth(opExpr->args, 1));
    tp = SearchSysCache1(OPEROID, ObjectIdGetDatum(operatorid));
    if (!HeapTupleIsValid(tp))
      elog(ERROR, "cache lookup failed for operator %u", operatorid);
    op = (Form_pg_operator)GETSTRUCT(tp);
    ReleaseSysCache(tp);

    swapOperandsAsNeeded(&l, &r, &operatorid, base_relids);
    if (IsA(l, Var) && bms_is_member(((Var *)l)->varno, base_relids) &&
        ((Var *)l)->varattno >= 1) {
      result = makeNode(ScalarArrayOpExpr);
      result->opno = operatorid;
      result->opfuncid = op->oprcode;
      result->useOr = opExpr->useOr;
      result->args = lappend(result->args, l);
      result->args = lappend(result->args, r);
      result->location = opExpr->location;
    }
  }
  return result;
}

/*
 * Extract conditions that can be pushed down, as well as the parameters.
 *
 */
void extractRestrictions(Relids base_relids, Expr *node, List **quals)
{
  //elog(WARNING, "extractRestrictions: - %s",nodeToString(node));
  switch (nodeTag(node)) {

    case T_OpExpr:
      if(extractClauseFromOpExpr(base_relids, (OpExpr *)node, quals))
        return;
      break;

    case T_NullTest:
      if(extractClauseFromNullTest(base_relids, (NullTest *)node, quals))
        return;
      break;

    case T_ScalarArrayOpExpr:
      if(extractClauseFromScalarArrayOpExpr(base_relids, (ScalarArrayOpExpr *)node, quals))
        return;
      break;

    case T_BoolExpr:
      if (extractClauseBoolVarFromBoolExpr(base_relids, (BoolExpr *)node, quals))
        return;

      if(extractClauseWithNullFromBoolExpr(base_relids, (BoolExpr *)node, quals))
         return;
      break;

    case T_Var:
      if(extractClauseFromBoolVar(base_relids, (Var*) node, quals, true))
        return;
      break;

    default: {
    } break;
  }
  ereport(WARNING, (errmsg("unsupported expression for "
                           "extractClauseFrom"),
                    errdetail("%s", nodeToString(node))));
}

/*
 *	Build an intermediate value representation for an OpExpr,
 *	and append it to the corresponding list (quals, or params).
 *
 *	The quals list consist of list of the form:
 *
 *	- Const key: the column index in the cinfo array
 *	- Const operator: the operator representation
 *	- Var or Const value: the value.
 */
bool extractClauseFromOpExpr(Relids base_relids, OpExpr *op, List **quals)
{
  Var *left;
  Expr *right;

  /* Use a "canonical" version of the op expression, to ensure that the */
  /* left operand is a Var on our relation. */
  op = canonicalOpExpr(op, base_relids);
  if (!op)
    return false;

  left = list_nth(op->args, 0);
  right = list_nth(op->args, 1);

  /* Do not add it if it either contains a mutable function, or makes */
  /* self references in the right hand side. */
  if (contain_volatile_functions((Node *)right))
    return true;

  if (bms_is_subset(base_relids, pull_varnos((Node *)right)))
    return true;

  *quals = lappend(*quals, makeQual(left->varattno, getOperatorString(op->opno),
                                    right, false, /*use or*/ false,
                                    /*include missing*/ false));

  return true;
}

bool extractClauseFromScalarArrayOpExpr(Relids base_relids, ScalarArrayOpExpr *op, List **quals)
{
  Var *left;
  Expr *right;

  op = canonicalScalarArrayOpExpr(op, base_relids);
  if (!op)
    return false;

  left = list_nth(op->args, 0);
  right = list_nth(op->args, 1);

  // elog(WARNING, "Check var right");
  if (contain_volatile_functions((Node *)right))
    return false;

  if (bms_is_subset(base_relids, pull_varnos((Node *)right)))
    return false;

  *quals = lappend(*quals,
                   makeQual(left->varattno, getOperatorString(op->opno), right,
                            true, op->useOr, /*include missing*/ false));

  return true;
}

/*
 *	Convert a "NullTest" (IS NULL, or IS NOT NULL)
 *	to a suitable intermediate representation.
 */
bool extractClauseFromNullTest(Relids base_relids, NullTest *node, List **quals)
{
  MulticornBaseQual *result;
  char *opname = NULL;

  if (node->nulltesttype == IS_NULL) {
    opname = "=";
  } else {
    opname = "<>";
  }

  if (IsA(node->arg, ScalarArrayOpExpr)) {
    // for list types expressions look like this
    //-2147483648 = ALL(p_tumornormalforreliabilitytesting_1.distoftx) IS NULL

    ScalarArrayOpExpr *scallarOp = (ScalarArrayOpExpr *)node->arg;
    scallarOp = canonicalScalarArrayOpExpr(scallarOp, base_relids);

    Var *left = (Var *)list_nth(scallarOp->args, 0);
    Expr *right = (Expr *)list_nth(scallarOp->args, 1);

    if (contain_volatile_functions((Node *)right))
      return false;

    if (bms_is_subset(base_relids, pull_varnos((Node *)right)))
      return false;

    // we could pull the const out of the right operator and compare wit
    // INT_MISSING, FLOAT_MISSING, etc
    if (strcmp(getOperatorString(scallarOp->opno), "=") != 0)
      return false;

    result = makeQual(left->varattno, opname,
                      (Expr *)makeNullConst(INT4OID, -1, InvalidOid), false,
                      /*use or*/ false,
                      /*include missing*/ node->nulltesttype == IS_NULL);

  } else if (IsA(node->arg, Var)) {

    Var *var = (Var *)node->arg;

    if (var->varattno < 1)
      return false;

    result = makeQual(var->varattno, opname,
                      (Expr *)makeNullConst(INT4OID, -1, InvalidOid), false,
                      /*use or*/ false,
                      /*include missing*/ node->nulltesttype == IS_NULL);
  }
  *quals = lappend(*quals, result);

  return true;
}

/*
 *  When a variable is the entire expression (bool fields) we extract the value
 *  and save the desired state in the operator [True|False]
 */
bool extractClauseFromBoolVar(Relids base_relids, Var *varNode, List **quals, bool value)
{
  if(varNode->vartype != BOOLOID)
    return false;

  char* opname = NULL; 
  if(value)
    opname = "True";
  else
    opname = "False";

  *quals = lappend(*quals,
                  makeQual(varNode->varattno, opname, (Expr*)varNode, /* is array*/ false,
                           /*useOr*/ false, /*includeMissing*/ false));
  return true;
}

bool extractClauseBoolVarFromBoolExpr(Relids base_relids, BoolExpr* op, List **quals)
{
  // handle inverted clauses on boolean fields(i.e. field == False)

  if (op->boolop != NOT_EXPR)
    return false;

  if (list_length(op->args) != 1)
    return false;

  Expr *arg = (Expr *)list_nth(op->args, 0);

  if (!IsA(arg, Var))
    return false;

  return extractClauseFromBoolVar(base_relids, (Var *)arg, quals,
                                  /*not this*/ false);
}

/*
 *  Convert a BoolExpr [Not|Or|And] to a intermediate representation
 */
bool extractClauseWithNullFromBoolExpr(Relids base_relids, BoolExpr *node,
                                      List **quals) {
  if(node->boolop != OR_EXPR)
    return false;

  // only lookgin for nodes with an expression followed by a "NullTest"
  if (list_length(node->args) != 2)
    return false;

  List* exprList = NIL;
  List* nullQualList = NIL;

  ListCell *lc;
  foreach (lc, node->args) {
    Expr *subNode = lfirst(lc);

    switch (nodeTag(subNode)) {
    case T_ScalarArrayOpExpr:
      if (!extractClauseFromScalarArrayOpExpr(base_relids, (ScalarArrayOpExpr *)subNode, &exprList))
        return false;
      break;

    case T_OpExpr:
      if (!extractClauseFromOpExpr(base_relids, (OpExpr *)subNode, &exprList))
        return false;

      break;
    case T_NullTest:
      if(length(nullQualList) > 0)
        return false;
      if(!extractClauseFromNullTest(base_relids, (NullTest*)subNode, &nullQualList))
        return false;
      break;

    case T_BoolExpr:
      if (!extractClauseBoolVarFromBoolExpr(base_relids, (BoolExpr *)subNode, &exprList))
        return false;
    // this happens with sample filters -> may need to revisit
    // there is a nested 'sample is not null' and 'sample value is not null'
    // this would be added to the null qual list if collapsed
    default:
      return false;
    }
  }

  // check the is null qual
  if (length(exprList) != 1 || length(nullQualList) != 1)
    return false;

  MulticornBaseQual *exprQual = list_nth(exprList, 0);
  MulticornBaseQual *nullQual = list_nth(nullQualList, 0);

  if(exprQual->varattno != nullQual->varattno)
    return false;

  exprQual->includeMissing = nullQual->includeMissing;
  *quals = lappend(*quals, exprQual);

  return true;
}
/*
 *	Returns a "Value" node containing the string name of the column from a var.
 */
Value *colnameFromVar(Var *var, PlannerInfo *root, MulticornPlanState *planstate)
{
  RangeTblEntry *rte = rte = planner_rt_fetch(var->varno, root);
  char *attname = get_attname(rte->relid, var->varattno);

  if (attname == NULL) {
    return NULL;
  } else {
    return makeString(attname);
  }
}

/*
 *	Build an opaque "qual" object.
 */
MulticornBaseQual *makeQual(AttrNumber varattno, char *opname, Expr *value,
                            bool isarray, bool useOr, bool includeMissing) {
  MulticornBaseQual *qual;

  elog(INFO, "make qual expr: %s", nodeToString(value));
  switch (value->type) {
    case T_Const:
      qual = palloc0(sizeof(MulticornConstQual));
      qual->right_type = T_Const;
      qual->typeoid = ((Const *)value)->consttype;
      ((MulticornConstQual *)qual)->value = ((Const *)value)->constvalue;
      ((MulticornConstQual *)qual)->isnull = ((Const *)value)->constisnull;
      break;
    case T_Var:
      qual = palloc0(sizeof(MulticornVarQual));
      qual->right_type = T_Var;
      ((MulticornVarQual *)qual)->rightvarattno = ((Var *)value)->varattno;
      break;
    default:
      qual = palloc0(sizeof(MulticornParamQual));
      qual->right_type = T_Param;
      ((MulticornParamQual *)qual)->expr = value;
      qual->typeoid = InvalidOid;
      break;
  }

  qual->varattno = varattno;
  qual->opname = opname;
  qual->isArray = isarray;
  qual->useOr = useOr;
  qual->includeMissing = includeMissing;
  return qual;
}

void collapseNullQuals(List **quals)
{
  //collapse nodes that have the same value ->varattno
  //where one node is a const and the other is a qual

  //all of the other atts should match
  List* trimmedList = list_copy(*quals);

  ListCell *lc;
  foreach(lc, *quals){

    MulticornBaseQual * current = lfirst(lc);

    if (current->right_type == T_Const &&
        ((MulticornConstQual *)current)->isnull)
      continue;


    ListCell *tlc;
    foreach(tlc, trimmedList){
      MulticornBaseQual * qual = lfirst(tlc);
      if (qual->varattno != current->varattno)
        continue;
      if (qual->right_type != T_Const)
        continue;
      // these should be set to the default of false for null checks
      if (qual->useOr || qual->isArray)
        continue;
      if(!((MulticornConstQual*)qual)->isnull)
        continue;

      // else we can colapse this null check into the is missing of current qual
      elog(INFO, "collapsed null test node. Include missing %d", qual->includeMissing);
      current->includeMissing = qual->includeMissing;
      trimmedList = list_delete_ptr(trimmedList, qual);
      break;
    }
  }

  //use the trimmed list
  *quals = trimmedList;
}

/*
 *	Test wheter an attribute identified by its relid and attno
 *	is present in a list of restrictinfo
 */
bool isAttrInRestrictInfo(Index relid, AttrNumber attno, RestrictInfo *restrictinfo)
{
  List *vars = pull_var_clause(
      (Node *)restrictinfo->clause, PVC_RECURSE_AGGREGATES, PVC_RECURSE_PLACEHOLDERS);
  ListCell *lc;

  foreach (lc, vars) {
    Var *var = (Var *)lfirst(lc);

    if (var->varno == relid && var->varattno == attno) {
      return true;
    }
  }
  return false;
}

List *clausesInvolvingAttr(Index relid, AttrNumber attnum, EquivalenceClass *ec)
{
  List *clauses = NULL;

  /*
   * If there is only one member, then the equivalence class is either for
   * an outer join, or a desired sort order. So we better leave it
   * untouched.
   */
  if (ec->ec_members->length > 1) {
    ListCell *ri_lc;

    foreach (ri_lc, ec->ec_sources) {
      RestrictInfo *ri = (RestrictInfo *)lfirst(ri_lc);

      if (isAttrInRestrictInfo(relid, attnum, ri)) {
        clauses = lappend(clauses, ri);
      }
    }
  }
  return clauses;
}

void findPaths(PlannerInfo *root, RelOptInfo *baserel, List *possiblePaths, int startupCost)
{
  ListCell *lc;

  foreach (lc, possiblePaths) {
    List *item = lfirst(lc);
    List *attrnos = linitial(item);
    ListCell *attno_lc;
    int nbrows = ((Const *)lsecond(item))->constvalue;
    List *allclauses = NULL;
    Bitmapset *outer_relids = NULL;

    /* Armed with this knowledge, look for a join condition */
    /* matching the path list. */
    /* Every key must be present in either, a join clause or an */
    /* equivalence_class. */
    foreach (attno_lc, attrnos) {
      AttrNumber attnum = lfirst_int(attno_lc);
      ListCell *lc;
      List *clauses = NULL;

      /* Look in the equivalence classes. */
      foreach (lc, root->eq_classes) {
        EquivalenceClass *ec = (EquivalenceClass *)lfirst(lc);
        List *ec_clauses = clausesInvolvingAttr(baserel->relid, attnum, ec);

        clauses = list_concat(clauses, ec_clauses);
        if (ec_clauses != NIL) {
          outer_relids = bms_union(outer_relids, ec->ec_relids);
        }
      }
      /* Do the same thing for the outer joins */
      foreach (lc, list_union(root->left_join_clauses, root->right_join_clauses)) {
        RestrictInfo *ri = (RestrictInfo *)lfirst(lc);

        if (isAttrInRestrictInfo(baserel->relid, attnum, ri)) {
          clauses = lappend(clauses, ri);
          outer_relids = bms_union(outer_relids, ri->outer_relids);
        }
      }
      /* We did NOT find anything for this key, bail out */
      if (clauses == NIL) {
        allclauses = NULL;
        break;
      } else {
        allclauses = list_concat(allclauses, clauses);
      }
    }
    /* Every key has a corresponding restriction, we can build */
    /* the parameterized path and add it to the plan. */
    if (allclauses != NIL) {
      Bitmapset *req_outer = bms_difference(outer_relids, bms_make_singleton(baserel->relid));
      ParamPathInfo *ppi;
      ForeignPath *foreignPath;

      if (!bms_is_empty(req_outer)) {
        ppi = makeNode(ParamPathInfo);
        ppi->ppi_req_outer = req_outer;
        ppi->ppi_rows = nbrows;
        ppi->ppi_clauses = list_concat(ppi->ppi_clauses, allclauses);
        foreignPath = create_foreignscan_path(root, baserel, nbrows, startupCost,
                                              nbrows * baserel->width,
                                              NIL,
                                              NULL,
#if PG_VERSION_NUM >= 90500
                                              NULL,
#endif
                                              NULL);

        foreignPath->path.param_info = ppi;
        add_path(baserel, (Path *)foreignPath);
      }
    }
  }
}
 
