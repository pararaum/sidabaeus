#! /usr/bin/env python

"""
Get SID data and write it into a file.
"""

import psycopg2
import sys

def get_sids(crsr, slist):
    crsr.execute("SELECT sid,data FROM files WHERE sid in (" + ','.join("%d" % i for i in slist) + ");")
    records = crsr.fetchall()
    #print records, records[0][1]
    return records


def main(args):
    try:
        con = psycopg2.connect("")
    except psycopg2.OperationalError:
        print "Error while connecting to database. Perhaps you forgot to set the environment variables?"
        raise
    crsr = con.cursor()
    sids = get_sids(crsr, [int(i) for i in args])
    for sidp in sids:
        sid, data = sidp
        print " $%04X" % sid
        with file("%d.sid" % sid, "w") as out:
            out.write(str(data))
    
if __name__ == "__main__":
    main(sys.argv[1:])
