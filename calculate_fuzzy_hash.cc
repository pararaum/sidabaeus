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
  return tlsh.getHash();
}


void insert_hash(pqxx::work &txn, unsigned long sid, const std::string &hash) {
  std::ostringstream query;

  query << "INSERT INTO fuzzy_tlsh (sid, hash) VALUES ("
	<< sid << ','
	<< "decode(" << txn.quote(hash) << ", 'hex')"
	<< ");";
  txn.exec(query.str());
}


int run(pqxx::connection &conn, char **begin, char **end, const gengetopt_args_info &args) {
  std::ostringstream query;
  pqxx::result result;

  query << "SELECT sid, data, length(data) FROM files WHERE sid NOT IN"
        << " (SELECT sid FROM fuzzy_tlsh) AND data NOTNULL"
        << ';';
  try {
    pqxx::work txn(conn, "calculate TLSH");
    pqxx::icursorstream cursor(txn, query.str(), "cursor for TLSH", RESULT_STRIDE);
    while(cursor >> result) {
      for(auto row : result) {
	unsigned long sid = row[0].as<unsigned long>();
	unsigned long dsize = row[2].as<unsigned long>();
	std::cout << boost::format("$%06lx $%04lX\n") % sid % dsize;
	if(dsize >= MIN_DATA_LENGTH) {
	  pqxx::binarystring binstr(row["data"]);
	  std::string hash(calculate_tlsh(binstr));
	  std::cout << '\t' << hash << std::endl;
	  insert_hash(txn, sid, hash);
	}
      }
    }
    // std::for_each(begin, end, [](char *fname) {
    // 	std::string s(fname);
    // 	Tlsh tlsh;
	
    // 	std::cout << fname << std::endl;
    // 	tlsh.update((const unsigned char *)(s.c_str()), s.size());
    // 	tlsh.update((const unsigned char *)(s.c_str()), s.size());
    // 	tlsh.update((const unsigned char *)(s.c_str()), s.size());
    // 	tlsh.update((const unsigned char *)(s.c_str()), s.size());
    // 	tlsh.update((const unsigned char *)(s.c_str()), s.size());
    // 	tlsh.update((const unsigned char *)(s.c_str()), s.size());
    // 	tlsh.update((const unsigned char *)(s.c_str()), s.size());
    // 	tlsh.update((const unsigned char *)(s.c_str()), s.size());
    // 	tlsh.final((const unsigned char *)(s.c_str()), s.size());
    // 	std::cout << tlsh.getHash() << std::endl;
    // 	std::string hash(tlsh.getHash());
    // 	std::cout << (const void *)(tlsh.getHash()) << '\t' << hash.size() << std::endl;
    //   });
    txn.commit();
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
