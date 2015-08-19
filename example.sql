CREATE EXTENSION tsf_fdw;

CREATE SERVER tsf_server FOREIGN DATA WRAPPER tsf_fdw;

CREATE FOREIGN TABLE low_level (
       _id integer,
       Chr text,
       Start integer,
       Stop integer,
       IntField integer,
       Int64Field bigint,
       FloatField real,
       DoubleField double precision,
       BoolField boolean,
       StringField text,
       IntArrayField integer[],
       FloatArrayField real[],
       DoubleArrayField double precision[],
       StringArrayField text[],
       EnumField text,
       EnumArrayField text[]
       )
       SERVER tsf_server
       OPTIONS (filename '/Users/grudy/dev/tsf_fdw/tsf-c-1.0/tests/Low_level.tsf', sourceid '1');
