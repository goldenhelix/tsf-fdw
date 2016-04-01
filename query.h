/*-------------------------------------------------------------------------
 *
 * query.h
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

#include "postgres.h"
#include "nodes/pg_list.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_foreign_table.h"
#include "commands/defrem.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "funcapi.h"
#include "lib/stringinfo.h"
#include "nodes/relation.h"
#include "utils/builtins.h"
#include "catalog/pg_type.h"
#include "utils/syscache.h"
#include "access/relscan.h"
#include "nodes/makefuncs.h"
#include "nodes/bitmapset.h"

#ifndef PG_QUERY_H
#define PG_QUERY_H

/* Data structures */

typedef struct ConversionInfo {
  char *attrname;
  FmgrInfo *attinfunc;
  FmgrInfo *attoutfunc;
  Oid atttypoid;
  Oid attioparam;
  int32 atttypmod;
  int attnum;
  bool is_array;
  int attndims;
  bool need_quote;
} ConversionInfo;

typedef struct MulticornPlanState {
  Oid foreigntableid;
  AttrNumber numattrs;
  List *target_list;
  List *qual_list;
  int startupCost;
  ConversionInfo **cinfos;
} MulticornPlanState;

typedef struct MulticornBaseQual {
  AttrNumber varattno;
  NodeTag right_type;
  Oid typeoid;
  char *opname;
  bool isArray;
  bool useOr; // ANY on array operators
} MulticornBaseQual;

typedef struct MulticornConstQual {
  MulticornBaseQual base;
  Datum value;
  bool isnull;
} MulticornConstQual;

typedef struct MulticornVarQual {
  MulticornBaseQual base;
  AttrNumber rightvarattno;
} MulticornVarQual;

typedef struct MulticornParamQual {
  MulticornBaseQual base;
  Expr *expr;
} MulticornParamQual;

/* query.c */
void extractRestrictions(Relids base_relids, Expr *node, List **quals);
List *extractColumns(List *reltargetlist, List *restrictinfolist);
void initConversioninfo(ConversionInfo **cinfo, AttInMetadata *attinmeta);

Value *colnameFromVar(Var *var, PlannerInfo *root, MulticornPlanState *state);

void findPaths(PlannerInfo *root, RelOptInfo *baserel, List *possiblePaths, int startupCost);

#endif
