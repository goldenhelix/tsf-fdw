# tsf_fdw/Makefile
#
# Copyright 2015 Golden Helix, Inc.
#

MODULE_big = tsf_fdw

TSF_DRIVER = tsf-c-1.0
TSF_PATH = $(TSF_DRIVER)/src
TSF_OBJS = $(TSF_PATH)/tsf.os \
	$(TSF_PATH)/sqlite3/sqlite3.os \
	$(TSF_PATH)/jansson/dump.o \
	$(TSF_PATH)/jansson/error.o \
	$(TSF_PATH)/jansson/hashtable.o \
	$(TSF_PATH)/jansson/load.o \
	$(TSF_PATH)/jansson/memory.o \
	$(TSF_PATH)/jansson/pack_unpack.o \
	$(TSF_PATH)/jansson/strbuffer.o \
	$(TSF_PATH)/jansson/strconv.o \
	$(TSF_PATH)/jansson/utf.o \
	$(TSF_PATH)/jansson/value.o \
	$(TSF_PATH)/blosc/blosc.o \
	$(TSF_PATH)/blosc/blosclz.o \
	$(TSF_PATH)/blosc/shuffle.o

PG_CPPFLAGS = --std=c99 -ggdb -I$(TSF_PATH) -I$(TSF_PATH)/jansson -I$(TSF_PATH)/blosc
SHLIB_LINK = -lz
OBJS = tsf_fdw.o stringbuilder.o $(TSF_OBJS)

EXTENSION = tsf_fdw
DATA = tsf_fdw--1.0.sql

$(TSF_DRIVER)/%.os:
	$(MAKE) -C $(TSF_DRIVER) $*.os

#
# Users need to specify their Postgres installation path through pg_config. For
# example: /usr/local/pgsql/bin/pg_config or /usr/lib/postgresql/9.1/bin/pg_config
#

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
