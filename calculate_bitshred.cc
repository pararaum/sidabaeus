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
#include "calculate_bitshred.cmdline.h"
#include "bitshred.hh"
#include "hash.hh"

typedef std::map<unsigned long, pqxx::binarystring> Sid2Data;


Sid2Data get_sids_without(pqxx::connection &conn, unsigned int m, unsigned int n, const std::string &hash) {
  Sid2Data sids;
  pqxx::work txn(conn, "get sids");
  std::ostringstream query;

  query << "SELECT sid,data FROM files WHERE sid NOT IN"
	<< " (SELECT sid FROM bitshred WHERE"
	<< " m = " << txn.quote(m)
	<< " AND n = " << txn.quote(n)
	<< " AND hash = " << txn.quote(hash)
	<< ") AND data NOTNULL";
  //std::cout << query.str() << std::endl;
  pqxx::result result(txn.exec(query.str()));
  for(auto row : result) {
    pqxx::binarystring binstr(row["data"]);
    unsigned long sid = row["sid"].as<unsigned long>();
    sids.emplace(sid, binstr);
  }
  return sids;
}


BitshredType calculate_bitshred(const pqxx::binarystring &data, unsigned int m, unsigned int n, const std::function<uint32_t(const uint8_t *, size_t)> &hashfun) {
  BitshredType bitshred(m);
  pqxx::binarystring::const_iterator dbegin(data.begin());
  pqxx::binarystring::const_iterator dend(data.end());

  if(std::distance(dbegin, dend) < n) throw std::invalid_argument("not enough bytes for bitshred");
  for(auto ptr = dbegin; ptr < dend - n; ++ptr) {
    uint32_t hash = hashfun(&ptr[0], n);
    bitshred[hash % m] = 1;
  }
  return bitshred;
}


std::ostream &operator<<(std::ostream &out, const BitshredType &bitshred) {
  for(auto i : bitshred) out << (i ? '1' : '0');
  return out;
}

unsigned store_bitshred(pqxx::connection &conn, unsigned long sid, unsigned int m, unsigned int n, const std::string &hashname, const BitshredType &bitshred) {
  unsigned int bits = 0;
  pqxx::work txn(conn, "store bitshred");
  std::ostringstream query;
  std::ostringstream shred;
  std::string binstr;
  char buf[4];

  for(unsigned int i = 0; i < bitshred.size(); i += 8) {
    unsigned val = (bitshred[i] ? 128 : 0   )
      		 | (bitshred[i + 1] ? 64 : 0)
      		 | (bitshred[i + 2] ? 32 : 0)
      		 | (bitshred[i + 3] ? 16 : 0)
      		 | (bitshred[i + 4] ?  8 : 0)
      		 | (bitshred[i + 5] ?  4 : 0)
      		 | (bitshred[i + 6] ?  2 : 0)
      		 | (bitshred[i + 7] ?  1 : 0);
    sprintf(buf, "%02X", val);
    binstr += buf;
  }
  query << "INSERT INTO bitshred (sid, m, n, hash, bitshred) VALUES ("
	<< txn.quote(sid) << ", "
	<< txn.quote(m) << ", "
	<< txn.quote(n) << ", "
	<< txn.quote(hashname) << ", "
	<< "decode(" << txn.quote(binstr) << ", 'hex') );";
  //std::cout << query.str() << std::endl;
  txn.exec(query.str());
  txn.commit();
  //bits = bitshred.count();
  bits = std::count(bitshred.begin(), bitshred.end(), 1);
  return bits;
}

int run(pqxx::connection &conn, const gengetopt_args_info &args) {
  std::map<std::string, std::function<uint32_t(const uint8_t *, size_t)> > hash_functions {
    { "jenkins", &jenkins_one_at_a_time_hash },
    { "djb2", &djb2_hash },
    { "djb2xor", &djb2xor_hash },
    { "sbox", &sbox_hash}
  };
  try {
    auto hash_function = hash_functions.at(args.hash_arg);
    auto siddata(get_sids_without(conn, args.size_arg, args.ngram_arg, args.hash_arg));
    std::cout << "SIDs loaded:" << siddata.size() << std::endl;
    for(auto const &i : siddata) {
      std::cout << boost::format("$%04X ") % i.first;
      BitshredType bitshred(calculate_bitshred(i.second, args.size_arg, args.ngram_arg, hash_function));
      unsigned int bits = store_bitshred(conn, i.first, args.size_arg, args.ngram_arg, args.hash_arg, bitshred);
      std::cout << boost::format("size=$%04x bits=$%04x %13.6e") % i.second.size() % bits % (static_cast<double>(bits) / bitshred.size());
      std::cout << std::endl;
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
  
  //if(std::getenv("SIDDB")) dbname = std::getenv("SIDDB");
  //if(std::getenv("SIDUSER")) dbname = std::getenv("SIDUSER");
  if(cmdline_parser(argc, argv, &args) != 0) return 1;
  try {
    connection_string << "dbname=" << args.dbname_arg << " user=" << args.dbuser_arg;
    if(args.dbhost_given) connection_string << " host=" << args.dbhost_arg;
    if(args.dbpass_given) connection_string << " password=" << args.dbpass_arg;
    pqxx::connection conn(connection_string.str());
    retval = run(conn, args);
  }
  catch (const std::exception &e) {
    std::cerr << "ERROR: " << e.what() << std::endl;
    return retval;
  }
  return retval;
}
