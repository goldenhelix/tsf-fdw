# tsf_fdw/Makefile
#
# Copyright 2015 Golden Helix, Inc.
#

MODULE_big = tsf_fdw

TSF_DRIVER = tsf-c-1.0

TSF_PATH = $(TSF_DRIVER)/src

SQLITE_PATH = $(TSF_PATH)/sqlite3
SQLITE_OBJS = $(SQLITE_PATH)/sqlite3.o

JANSSON_PATH = $(TSF_PATH)/jansson
JANSSON_OBJS = \
	$(JANSSON_PATH)/dump.o \
	$(JANSSON_PATH)/error.o \
	$(JANSSON_PATH)/hashtable.o \
	$(JANSSON_PATH)/load.o \
	$(JANSSON_PATH)/memory.o \
	$(JANSSON_PATH)/pack_unpack.o \
	$(JANSSON_PATH)/strbuffer.o \
	$(JANSSON_PATH)/strconv.o \
	$(JANSSON_PATH)/utf.o \
	$(JANSSON_PATH)/value.o

BLOSC_PATH = $(TSF_PATH)/blosc
BLOSC_OBJS = \
  $(BLOSC_PATH)/blosc.o \
  $(BLOSC_PATH)/blosclz.o \
  $(BLOSC_PATH)/shuffle.o

ZSTD_PATH = $(TSF_PATH)/zstd/lib
ZSTD_OBJS = \
  ${ZSTD_PATH}/common/entropy_common.o \
  ${ZSTD_PATH}/common/zstd_common.o \
  ${ZSTD_PATH}/common/xxhash.o \
  ${ZSTD_PATH}/common/fse_decompress.o \
  ${ZSTD_PATH}/compress/fse_compress.o \
  ${ZSTD_PATH}/compress/huf_compress.o \
  ${ZSTD_PATH}/compress/zbuff_compress.o \
  ${ZSTD_PATH}/compress/zstd_compress.o \
  ${ZSTD_PATH}/decompress/huf_decompress.o \
  ${ZSTD_PATH}/decompress/zbuff_decompress.o \
  ${ZSTD_PATH}/decompress/zstd_decompress.o \
  ${ZSTD_PATH}/dictBuilder/divsufsort.o \
  ${ZSTD_PATH}/dictBuilder/zdict.o

LZ4_PATH = $(TSF_PATH)//lz4/lib
LZ4_OBJS = \
  $(LZ4_PATH)/lz4.o \
  $(LZ4_PATH)/lz4hc.o \
  $(LZ4_PATH)/lz4frame.o \
  $(LZ4_PATH)/xxhash.o

TSF_OBJS = $(TSF_PATH)/tsf.o \
           $(SQLITE_OBJS) \
           $(JANSSON_OBJS) \
           $(BLOSC_OBJS) \
           $(ZSTD_OBJS) \
           $(LZ4_OBJS)

#TIP: For debugging purposes, build like:
#> OPTIMIZATION='-g -O0' make
OPTIMIZATION?=-O3

PG_CPPFLAGS = --std=c99 -Wno-declaration-after-statement $(OPTIMIZATION) -I$(TSF_PATH) -I$(JANSSON_PATH) -I$(BLOSC_PATH) -I$(ZSTD_PATH) -I$(ZSTD_PATH)/common -I$(LZ4_PATH)
SHLIB_LINK = -lz
OBJS = tsf_fdw.o query.o util.o stringbuilder.o $(TSF_OBJS)

EXTENSION = tsf_fdw
DATA = tsf_fdw--1.0.sql

#
# Users need to specify their Postgres installation path through pg_config. For
# example: /usr/local/pgsql/bin/pg_config or /usr/lib/postgresql/9.1/bin/pg_config
#

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
