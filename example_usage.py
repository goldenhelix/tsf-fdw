from __future__ import print_function
import psycopg2

import argparse
parser = argparse.ArgumentParser()
parser.add_argument("db", help="existing postgres database",
                    nargs='?', default="test_tsf_fdw")
parser.add_argument("filename", help="TSF file name",
                    nargs='?', default="/Volumes/MicroDrive/TestData/p1.tsf")
parser.add_argument("prefix", help="Table prefix to use",
                    nargs='?', default="table1")
args = parser.parse_args()



conn = psycopg2.connect("dbname='%s'" % args.db)
conn.set_isolation_level(0)

cur = conn.cursor()
try:
    cur.execute("CREATE EXTENSION tsf_fdw;")
except:
    pass
try:
    cur.execute("CREATE SERVER tsf_server FOREIGN DATA WRAPPER tsf_fdw;")
except:
    pass

cur.execute("select tsf_generate_schemas('%s', '%s');" % ( args.prefix, args.filename) )
r = cur.fetchall()[0][0]
cur.execute(r)
print(r)

