/* tsf_fdw/tsf_fdw--1.0.sql */

-- Copyright (c) 2015 Golden Helix, Inc.

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION tsf_fdw" to load this file. \quit

-- table_prefix, tsf_path, optional source id (otherwise all sources generated)
CREATE FUNCTION tsf_generate_schemas(text, text, integer default -1)
RETURNS text
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION tsf_fdw_handler()
RETURNS fdw_handler
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION tsf_fdw_validator(text[], oid)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER tsf_fdw
  HANDLER tsf_fdw_handler
  VALIDATOR tsf_fdw_validator;
