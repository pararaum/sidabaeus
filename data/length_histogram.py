#! /usr/bin/env python

"""
Draw a histogram of all sid sizes.

Try also: SELECT count(*),author FROM songs GROUP BY author ORDER BY count;

* http://stackoverflow.com/questions/5328556/histogram-matplotlib#5328669
"""

BINS = 512

import psycopg2
import matplotlib.pyplot as plt
import numpy as np

def get_sizes(crsr):
    crsr.execute("SELECT sid,length(data) FROM files ORDER BY length;")
    records = crsr.fetchall()
    return np.array([i[1] for i in records])

def get_per_author(crsr):
    crsr.execute("SELECT author,count(*) FROM songs GROUP BY author ORDER BY count;")
    records = crsr.fetchall()
    return np.array([i[1] for i in records])

def histogram(data, bins):
    hist, bins = np.histogram(data, bins=bins)
    width = 0.7 * (bins[1] - bins[0])
    center = (bins[:-1] + bins[1:]) / 2
    plt.bar(center, hist, align='center', width=width)
    plt.show()

def main():
    try:
        con = psycopg2.connect("")
    except psycopg2.OperationalError:
        print "Error while connecting to database. Perhaps you forgot to set the environment variables?"
        raise
    crsr = con.cursor()
    sizes = get_sizes(crsr)
    #print("mean    = %15.8e" % np.mean(sizes))
    print("average       = %15.8e" % np.average(sizes))
    print("std deviation = %15.8e" % np.std(sizes))
    print("median        = %15.8e" % np.median(sizes))
    histogram(sizes, BINS)
    histogram(get_per_author(crsr), BINS)
    
if __name__ == "__main__":
    main()
