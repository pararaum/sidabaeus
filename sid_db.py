#! /usr/bin/python

"""
Simple program to populate the sid database.

Storing all the bigram counts takes up a huge amount of disk space. As can be seen from the following query:

  SELECT *, pg_size_pretty(total_bytes) AS total
      , pg_size_pretty(index_bytes) AS INDEX
      , pg_size_pretty(toast_bytes) AS toast
      , pg_size_pretty(table_bytes) AS TABLE
    FROM (
    SELECT *, total_bytes-index_bytes-COALESCE(toast_bytes,0) AS table_bytes FROM (
        SELECT c.oid,nspname AS table_schema, relname AS TABLE_NAME
                , c.reltuples AS row_estimate
                , pg_total_relation_size(c.oid) AS total_bytes
                , pg_indexes_size(c.oid) AS index_bytes
                , pg_total_relation_size(reltoastrelid) AS toast_bytes
            FROM pg_class c
            LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE relkind = 'r'
    ) a
  ) a;

  >  16444 | public             | bigram_counts           |  4.06569e+07 |  3596656640 |  1764646912 |             |  1832009728 | 3430 MB    | 1683 MB    |            | 1747 MB

Therefore we will abandon the storage of all bigram and will have to calculate them on the fly.

"""

import argparse
import psycopg2
import sidformat

DBNAME = "chip"

def calc_bigram_counts(content):
    """
    Calculate bigram counts

    @param content: file content
    @return: dictionary of counts
    """
    counts = {}
    content_ = map(ord, content)
    for pos in range(len(content) - 1):
        x, y = content_[pos:pos + 2]
        try:
            counts[(x, y)] += 1
        except KeyError:
            counts[(x, y)] = 1
    return counts


def insert_file(crsr, fname, store):
    """
    Insert a file into the database.

    @param crsr: cursor object
    @param fname: file name
    @param store: if True then the file contents is also stored in the database
    """
    with file(fname) as inpf:
        content = inpf.read()
        try:
            data = sidformat.Psid(content)
        except RuntimeError, excp:
            raise RuntimeError("%s for '%s'" % (str(excp), fname))
        name = data.name
        author = data.author
        released = data.released
        #print data, name, author, released
        if store:
            crsr.execute("INSERT INTO files (filename, data) VALUES(%s, %s) RETURNING sid;", (fname, psycopg2.Binary(content)))
        else:
            crsr.execute("INSERT INTO files (filename) VALUES(%s) RETURNING sid;", (fname,))
        sid = int(crsr.fetchone()[0])
        crsr.execute("INSERT INTO songs (sid, name, author, released) VALUES(%s, %s, %s, %s);", (sid, name, author, released))
        #counts = calc_bigram_counts(content)
        #for ((fst, snd), count) in counts.iteritems():
        #    crsr.execute("INSERT INTO bigram_counts (sid, fst, snd, count) VALUES(%s, %s, %s, %s);", (sid, fst, snd, count))


def cli():
    """
    Command line interface, parsing

    @returns: ArgumentParser
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--dbname", help="database name", default=DBNAME)
    parser.add_argument("--dbhost", help="database host")
    parser.add_argument("--dbuser", help="database user")
    parser.add_argument("--dbpass", help="database password")
    parser.add_argument("--commit", help="Intermediately commit into database", default=False, type=bool)
    parser.add_argument("--ignore", help="Ignore errors on insert", default=False, type=bool)
    parser.add_argument("--store", help="Store file in database", default=False, type=bool)
    parser.add_argument("files", nargs='+')
    return parser.parse_args()

def main(cliargs):
    """
    Main function.

    @param cliargs: command line arguments
    """
    fnames = cliargs.files
    constr = "dbname=%s" % cliargs.dbname
    if cliargs.dbuser is not None:
        constr += " user=%s" % cliargs.dbuser
    if cliargs.dbhost is not None:
        constr += " host=%s" % cliargs.dbhost
    if cliargs.dbpass is not None:
        constr += " password=%s" % cliargs.dbpass
    conn = psycopg2.connect(constr)
    crsr = conn.cursor()
    for num, fname in enumerate(fnames):
        print("Processing %60s ($%04x/$%04x)" % (fname, num, len(fnames) - 1))
        try:
            insert_file(crsr, fname, cliargs.store)
            if cliargs.commit:
                conn.commit()
        except Exception, excp:
            print "Exception:", excp
            if not cliargs.ignore:
                raise
            else:
                conn.rollback()
                crsr = conn.cursor()
    print "Done!"
    conn.commit()

if __name__ == "__main__":
    main(cli())
