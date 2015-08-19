/*-------------------------------------------------------------------------
 *
 * tsf_fdw.h
 *
 * Type and function declarations for TSF foreign data wrapper.
 *
 * Copyright (c) 2015 Golden Helix, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef TSF_FDW_H
#define TSF_FDW_H

#include "tsf.h"

#include "fmgr.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_foreign_table.h"
#include "nodes/pg_list.h"
#include "nodes/relation.h"
#include "utils/timestamp.h"

/* Defines for valid option names */
#define OPTION_NAME_FILENAME "filename"
#define OPTION_NAME_SOURCEID "sourceid"

// TODO: Could add options about how matrix fields are collated

#define POSTGRES_TO_UNIX_EPOCH_DAYS (POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE)
#define POSTGRES_TO_UNIX_EPOCH_USECS (POSTGRES_TO_UNIX_EPOCH_DAYS * USECS_PER_DAY)

/*
 * TsfValidOption keeps an option name and a context. When an option is passed
 * into tsf_fdw objects (server and foreign table), we compare this option's
 * name and context against those of valid options.
 */
typedef struct TsfValidOption {
  const char *optionName;
  Oid optionContextId;

} TsfValidOption;

/* Array of options that are valid for tsf_fdw */
static const uint32 ValidOptionCount = 2;
static const TsfValidOption ValidOptionArray[] = {
    /* foreign table options */
    {OPTION_NAME_FILENAME, ForeignTableRelationId},
    {OPTION_NAME_SOURCEID, ForeignTableRelationId},
};

/*
 * TsfFdwOptions holds the option values to be used when connecting to Tsf.
 * To resolve these values, we first check foreign table's options, and if not
 * present, we then fall back to the default values specified above.
 */
typedef struct TsfFdwOptions {
  char *filename;
  int sourceId; //-1 is auto-detect (first)
} TsfFdwOptions;

/* Function declarations for foreign data wrapper */
extern Datum tsf_fdw_handler(PG_FUNCTION_ARGS);
extern Datum tsf_fdw_validator(PG_FUNCTION_ARGS);

#endif /* TSF_FDW_H */
