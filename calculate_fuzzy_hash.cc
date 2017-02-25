#include <algorithm>
#include <iostream>
#include <boost/format.hpp>
#include <pqxx/pqxx>
#include <set>
#include <vector>
#include <sstream>
#include <cstdlib>
#include <cmath>
#include <cstdio>
#include <bitset>
#include <stdexcept>
#include <tlsh.h>
#include "calculate_fuzzy_hash.cmdline.h"

#define RESULT_STRIDE 23


std::string calculate_tlsh(const pqxx::binarystring &binstr) {
  Tlsh tlsh;
  tlsh.final(binstr.data(), binstr.size());
  std::string hash(tlsh.getHash());
  if(hash.empty()) throw std::invalid_argument("empty TLSH hash");
  return hash;
}


void insert_tlsh(pqxx::work &txn, unsigned long sid, const std::string &hash) {
  std::ostringstream query;

  query << "INSERT INTO fuzzy_tlsh (sid, hash) VALUES ("
	<< sid << ','
	<< "decode(" << txn.quote(hash) << ", 'hex')"
	<< ");";
  txn.exec(query.str());
}


void find_similar_tlsh(pqxx::work &txn, const std::string &hash, unsigned int maxn) {
  pqxx::result result;
  Tlsh left, right;
  std::vector<std::pair<unsigned int, int> > distvec;
  pqxx::icursorstream cursor(txn, "SELECT sid, encode(hash, 'hex') AS h FROM fuzzy_tlsh", "cursor for TLSH", RESULT_STRIDE);

  left.fromTlshStr(hash.c_str());
  while(cursor >> result) {
    for(auto row : result) {
      right.fromTlshStr(row["h"].c_str());
      int diff = left.totalDiff(&right);
      //std::cout << row["sid"].c_str() << ' ' << row["h"].c_str() << ' ' << diff <<std::endl;
      distvec.push_back(std::make_pair(row["sid"].as<unsigned int>(), diff));
    }
  }
  std::sort(distvec.begin(), distvec.end(), [](const std::pair<unsigned int, int> &x, const std::pair<unsigned int, int> &y) { return x.second < y.second; });
  if(distvec.size() > maxn) distvec.resize(maxn);
  for(auto i : distvec) {
    result = txn.exec((boost::format("SELECT sid,name,author,released,filename,length(data) FROM songs NATURAL JOIN files WHERE sid = %u") % i.first).str(), "natural join");
    std::cout << boost::format("%5u $%04X %3d %32s %32s %32s %s\n")
      % i.first % i.first % i.second
      % result[0]["name"]
      % result[0]["author"]
      % result[0]["released"]
      % result[0]["filename"]
      ;
  }
}

int run(pqxx::connection &conn, char **begin, char **end, const gengetopt_args_info &args) {
  std::ostringstream query;
  pqxx::result result;
  unsigned long sid;

  if(std::string(args.hash_arg) != "tlsh") throw std::runtime_error("only tlsh allowed");
  query << "SELECT sid, data, length(data) FROM files WHERE sid NOT IN"
        << " (SELECT sid FROM fuzzy_tlsh) AND data NOTNULL AND length(data) >= " << MIN_DATA_LENGTH
	<< " LIMIT " << RESULT_STRIDE
        << ';';
  try {
    do {
      pqxx::work txn(conn, "calculate TLSH");
      //Cursor needs nested transactions!?
      //pqxx::icursorstream cursor(txn, query.str(), "cursor for TLSH", RESULT_STRIDE);
      result = txn.exec(query.str());
      for(auto row : result) {
	unsigned long dsize = row[2].as<unsigned long>();
	sid = row[0].as<unsigned long>();
	std::cout << boost::format("$%06lx $%04lX\n") % sid % dsize;
	if(dsize >= MIN_DATA_LENGTH) {
	  pqxx::binarystring binstr(row["data"]);
	  std::string hash(calculate_tlsh(binstr));
	  std::cout << '\t' << hash << std::endl;
	  insert_tlsh(txn, sid, hash);
	}
      }
      txn.commit();
    } while(!result.empty());
    //And direct query
    if(begin < end) {
      std::ostringstream search;
      pqxx::work txn(conn, "search query TLSH");

      search << "SELECT sid, encode(hash, 'hex') FROM fuzzy_tlsh WHERE sid in (";
      do {
	search << *begin++;
	if(begin < end) search << ',';
      } while(begin < end);
      search << ");";
      std::cout << search.str() << std::endl;
      pqxx::result searchresult(txn.exec(search.str()));
      for(auto row : searchresult) {
	sid = row["sid"].as<unsigned long>();
	std::string hash(row[1].c_str());
	std::cout << boost::format("%6ld '%s'\n") % sid % hash;
	find_similar_tlsh(txn, hash, args.maximum_dist_arg);
      }
    }
  }
  catch(const std::exception &excp) {
    std::cerr << "Exception: " << excp.what() << std::endl;
  }
 return 0;
}


int main(int argc, char **argv) {
  std::ostringstream connection_string;
  std::string dbname("chip");
  std::string dbuser("chip");
  std::string dbpass;
  std::string dbhost;
  int retval = -1;
  gengetopt_args_info args;
  
  if(cmdline_parser(argc, argv, &args) != 0) return 1;
  try {
    pqxx::connection conn(connection_string.str());
    retval = run(conn, &args.inputs[0], &args.inputs[args.inputs_num], args);
  }
  catch (const std::exception &e) {
    std::cerr << "ERROR: " << e.what() << std::endl;
    return retval;
  }
  return retval;
}
