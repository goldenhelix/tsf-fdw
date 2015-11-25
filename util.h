/*-------------------------------------------------------------------------
 *
 * util.h
 *
 * Ulities for parsing out qualifications, equivelence classes and join
 * relations from the query.
 *
 * Copyright (c) 2015 Golden Helix, Inc.
 *
 * Mostly copied from contrib/postgres_fdw from the Postgres code base
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

#ifndef PG_UTIL_H
#define PG_UTIL_H

/* Callback argument for ec_member_matches_foreign */
typedef struct
{
    Expr       *current;        /* current expr, or NULL if not yet found */
    List       *already_used;   /* expressions already dealt with */
} ec_member_foreign_arg;

bool
ec_member_matches_foreign(PlannerInfo *root, RelOptInfo *rel,
                          EquivalenceClass *ec, EquivalenceMember *em,
                          void *arg);

Expr *
find_em_expr_for_rel(EquivalenceClass *ec, RelOptInfo *rel);

#endif
