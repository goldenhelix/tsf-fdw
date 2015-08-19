/* tsf_fdw/tsf_fdw--1.0.sql */

-- Copyright (c) 2015 Golden Helix, Inc.

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION tsf_fdw" to load this file. \quit

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
