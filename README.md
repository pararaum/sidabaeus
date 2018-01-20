
Sidabaeus
=========

This project will contain statistical analysis of C64 SID songs and the tools
which are necessary to perform this analysis. A database will help us to
store all the information.

Links:

 * https://v2.pikacode.com/pararaum/sidabaeus
 * http://pararaum.blogspot.de/


Installation
============

Install the necessary packages (e.g. Debian/Ubuntu):

```
sudo apt-get install libboost-dev libpqxx-dev gengetopt libtlsh-dev python-psycopg2 libfuzzy-dev
```

Create a database user <user> and a database (example siddb):

```
createuser <user>
createdb -O <user> siddb
psql siddb < make_db.sql
```

And now you are ready to add files. You can use:
```
./sid_db.py --dbname=siddb $(find C64Music/ -name '*.sid')
```

