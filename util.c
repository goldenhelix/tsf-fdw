/*-------------------------------------------------------------------------
 *
 * util.c
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

#include "util.h"

#include "access/sysattr.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "commands/vacuum.h"
#include "foreign/fdwapi.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/planmain.h"
#include "optimizer/prep.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/var.h"
#include "parser/parsetree.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"

/*
 * Detect whether we want to process an EquivalenceClass member.
 *
 * This is a callback for use by generate_implied_equalities_for_column.
 */
bool
ec_member_matches_foreign(PlannerInfo *root, RelOptInfo *rel,
                          EquivalenceClass *ec, EquivalenceMember *em,
                          void *arg)
{
    ec_member_foreign_arg *state = (ec_member_foreign_arg *) arg;
    Expr       *expr = em->em_expr;

    /*
     * If we've identified what we're processing in the current scan, we only
     * want to match that expression.
     */
    if (state->current != NULL)
        return equal(expr, state->current);

    /*
     * Otherwise, ignore anything we've already processed.
     */
    if (list_member(state->already_used, expr))
        return false;

    /* This is the new target to process. */
    state->current = expr;
    return true;
}


/*
 * Find an equivalence class member expression, all of whose Vars, come from
 * the indicated relation.
 */
Expr *
find_em_expr_for_rel(EquivalenceClass *ec, RelOptInfo *rel)
{
    ListCell   *lc_em;

    foreach(lc_em, ec->ec_members)
    {
        EquivalenceMember *em = lfirst(lc_em);

        if (bms_equal(em->em_relids, rel->relids))
        {
            /*
             * If there is more than one equivalence member whose Vars are
             * taken entirely from this relation, we'll be content to choose
             * any one of those.
             */
            return em->em_expr;
        }
    }

    /* We didn't find any suitable equivalence class expression */
    return NULL;
}
