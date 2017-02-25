-- This is a song database for SIDs...

CREATE EXTENSION pg_trgm;

-- For each song we store an id, the songname, the author, when it was
-- released, and the filename we got the data from. Alternatively we
-- could also use CHAR(32) for name, author, released as this is the
-- maximum length in a PSID file.
--
-- We do not use tables for each of the information and join them for
-- each query as postgres (probably) will make use of compression and
-- we would have a huge amount of joins... Maybe later on.
--CREATE TABLE IF NOT EXISTS songs_OLD (sid SERIAL PRIMARY KEY, name TEXT NOT NULL, author TEXT NOT NULL, released TEXT NOT NULL, filename TEXT NOT NULL);
--CREATE INDEX songs_name_idx ON songs (name);
--CREATE INDEX songs_author_idx ON songs (author);
--CREATE INDEX songs_released_idx ON songs (released);

-- ##################################################################

-- This is the files tables. It contains the sid (storage id) and the
-- file name.
CREATE TABLE IF NOT EXISTS files (sid SERIAL PRIMARY KEY, filename TEXT NOT NULL UNIQUE, data bytea);
CREATE INDEX files_length_data ON files (length(data));

-- This table contains the counts for all bigrams found in the
-- file. Only a single unique tuple of the storage id, first byte,
-- second byte, and count is allowed. Count has to be greater than or
-- equal to zero, of course.
CREATE TABLE IF NOT EXISTS bigram_counts (sid INTEGER NOT NULL REFERENCES files ON DELETE CASCADE, fst SMALLINT NOT NULL, snd SMALLINT NOT NULL, count INTEGER NOT NULL, UNIQUE(sid,fst,snd,count), CHECK (count >= 0));
-- The next table stores the normalised distances of two bigram
-- counts. For this first the counts are taken from the bigram_counts
-- table, normalised to 1. Then the euclidian distance is calculated.
CREATE TABLE IF NOT EXISTS bigram_counts_distance (fst INTEGER NOT NULL REFERENCES files(sid) ON DELETE CASCADE, snd INTEGER NOT NULL REFERENCES files(sid) ON DELETE CASCADE, distance FLOAT, PRIMARY KEY(fst, snd), CHECK(fst < snd));

CREATE TABLE IF NOT EXISTS bigram2d_histo (sid INTEGER NOT NULL UNIQUE REFERENCES files ON DELETE CASCADE, bihi float ARRAY NOT NULL, CHECK (array_ndims(bihi) = 2));

CREATE TABLE IF NOT EXISTS songs (sid INTEGER NOT NULL UNIQUE REFERENCES files ON DELETE CASCADE, name TEXT NOT NULL, author TEXT NOT NULL, released TEXT NOT NULL);
-- IF NOT EXISTS in Postgresql >= 9.5
CREATE INDEX songs_name_idx ON songs (LOWER(name));
CREATE INDEX songs_author_idx ON songs (LOWER(author));
CREATE INDEX songs_released_idx ON songs (LOWER(released));
CREATE INDEX songs_name_tgrmidx ON songs USING GIST (name gist_trgm_ops);
CREATE INDEX songs_author_tgrmidx ON songs USING GIST (author gist_trgm_ops);
CREATE INDEX songs_released_tgrmidx ON songs USING GIST (released gist_trgm_ops);


-- Bitshred as feature storage
-- J. Jang, D. Brumley, S. Venkataraman, "BitShred: Feature Hashing Malware for Scalable Triage and Semantic Analysis", 2011.
-- J. Jang, D. Brumley, S. Venkataraman, "BitShred: Fast, Scalable Malware Triage", 2010.
-- Y. Li, et al., "Experimental Study of Fuzzy Hashing in Malware Clustering Analysis", 2015(?).
-- Calculate n-grams, hash them and set the corresponding bit in a bitmap of length m bits.
CREATE TABLE IF NOT EXISTS bitshred8192 (sid INTEGER NOT NULL REFERENCES files ON DELETE CASCADE, n INTEGER NOT NULL, hash TEXT NOT NULL, bitshred BIT(8192) NOT NULL, PRIMARY KEY (sid,n,hash), CHECK (n > 0));

-- The size of the bitshred can also be varied, therefore we use BIT VARYING to store m bits.
CREATE TABLE IF NOT EXISTS bitshred (sid INTEGER NOT NULL REFERENCES files ON DELETE CASCADE, m integer NOT NULL, n INTEGER NOT NULL, hash TEXT NOT NULL, bitshred BIT VARYING NOT NULL, PRIMARY KEY (sid,m,n,hash), CHECK (n > 0 AND length(bitshred) = m));


-- Table for the TLSH fuzzy-hash
CREATE TABLE IF NOT EXISTS fuzzy_tlsh (sid INTEGER NOT NULL REFERENCES files ON DELETE CASCADE, hash bytea NOT NULL, PRIMARY KEY (sid));


-- Very slow?
CREATE OR REPLACE FUNCTION calc_2d_histogram(asid integer) returns float array as $$
DECLARE
	arr float array;
	mymax float;
	myr RECORD;
BEGIN
	mymax := 10;
	arr := array_fill(0, ARRAY[256,256]);
	FOR myr IN SELECT fst, snd, count FROM bigram_counts WHERE sid = asid LOOP
	    -- RAISE NOTICE '% %',myr.fst,myr.snd;
	    arr[myr.fst+1][myr.snd+1] := myr.count / mymax;
	END LOOP;
	return arr;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION bitset_length(bit) RETURNS int AS $$
DECLARE i int;
        c int;
BEGIN
    c := 0;
    FOR i IN 1..length($1) LOOP
        IF substring($1, i, 1) = B'1' THEN
            c := c + 1;
        END IF;
    END LOOP;
    RETURN c;
END;
$$ LANGUAGE plpgsql;
