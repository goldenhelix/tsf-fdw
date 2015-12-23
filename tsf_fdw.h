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
#include "catalog/pg_attribute.h"
#include "nodes/pg_list.h"
#include "nodes/relation.h"
#include "utils/timestamp.h"

/* Defines for valid option names */
#define OPTION_NAME_FILENAME "filename"
#define OPTION_NAME_PATH "path"
#define OPTION_NAME_SOURCEID "sourceid"
#define OPTION_NAME_FIELDTYPE "fieldtype"
#define OPTION_NAME_FIELDIDX "fieldidx"
#define OPTION_NAME_MAPPINGID "mappingid"

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
static const uint32 ValidOptionCount = 8;
static const TsfValidOption ValidOptionArray[] = {
    /* foreign table options */
    {OPTION_NAME_FILENAME, ForeignTableRelationId},
    {OPTION_NAME_PATH, ForeignTableRelationId},
    {OPTION_NAME_SOURCEID, ForeignTableRelationId},
    {OPTION_NAME_FIELDTYPE, ForeignTableRelationId},

    {OPTION_NAME_FILENAME, AttributeRelationId},
    {OPTION_NAME_SOURCEID, AttributeRelationId},
    {OPTION_NAME_FIELDIDX, AttributeRelationId},
    {OPTION_NAME_MAPPINGID, AttributeRelationId},
};

/*
 * TsfFdwOptions holds the option values from the OPTIONS of the table
 * schema.
 *
 * The filename and sourceid are the default if no field-wise options
 * override them.
 */
typedef struct TsfFdwOptions {
  tsf_field_type fieldType;
  char *path;
  char *filename;
  int sourceId; //-1 is auto-detect (first)
} TsfFdwOptions;

/* Function declarations for foreign data wrapper */
extern Datum tsf_generate_schemas(PG_FUNCTION_ARGS);
extern Datum tsf_fdw_handler(PG_FUNCTION_ARGS);
extern Datum tsf_fdw_validator(PG_FUNCTION_ARGS);

#endif /* TSF_FDW_H */
