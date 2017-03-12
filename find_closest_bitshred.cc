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
#include <getopt.h>
#include <bitset>
#include <stdexcept>
#include "find_closest_bitshred.cmdline.h"
#include "bitshred.hh"

#define RESULT_STRIDE 89

typedef std::vector<std::pair<unsigned int, double> > DistancesVector;

unsigned bit_count(uint8_t x) {
  struct {
    unsigned operator()(uint8_t nibble) const {
      switch(nibble) {
      case 0:
	return 0;
      case 1:
      case 2:
      case 4:
      case 8:
	return 1;
      case 3:
      case 5:
      case 6:
      case 9:
      case 10:
      case 12:
	return 2;
      case 7:
      case 11:
      case 13:
      case 14:
	return 3;
      case 15:
	return 4;
      default:
	throw std::logic_error("nibble");
      }
    }
  } locf;
  return locf(x & 0xF) + locf(x >> 4);
}

DistancesVector calc_distances(pqxx::work &txn, unsigned int fstsid, unsigned int m, unsigned int n, const std::string &hashname, bool verbose) {
  std::ostringstream query;
  pqxx::result result;
  std::vector<std::pair<unsigned int, double> > distances;
  static auto plus_bitcount = [](unsigned s, uint8_t x) { return s + bit_count(x);};

  query << "SELECT bfst.sid, bsnd.sid, bfst.bitshred, bsnd.bitshred FROM bitshred bfst, bitshred bsnd WHERE"
	<< " (bfst.m, bfst.n, bfst.hash) = (bsnd.m, bsnd.n, bsnd.hash) AND bfst.sid != bsnd.sid"
	<< " AND bfst.sid = " << fstsid
	<< " AND bfst.m = " << m
	<< " AND bfst.n = " << n
	<< " AND bfst.hash = " << txn.quote(hashname)
	<< ';';
  //std::cout << query.str() << std::endl;
  /*
   * Warning! This query
   * 
   * pqxx::result result(txn.exec(query.str()));
   *
   * loads the *whole* data into memory, killing small computers like
   * RPi or Chip. Therefore we use this curser to load the results
   * sequentially and process them in batches of stride=12 rows.
   */
  pqxx::icursorstream cursor(txn, query.str(), "calc distances", RESULT_STRIDE);
  while(cursor >> result) {
    for(auto row : result) {
#ifndef NDEBUG
      unsigned int fstsid = row[0].as<unsigned int>();
#endif
      unsigned int sndsid = row[1].as<unsigned int>();
      pqxx::binarystring fst(row[2]);
      pqxx::binarystring snd(row[3]);      
      // std::vector<uint8_t> setdifference(fst.size());
      // std::vector<uint8_t> setunion(fst.size());
      if(fst.size() != snd.size()) throw std::runtime_error("fst.size() != snd.size");
      double diff_count = std::inner_product(fst.begin(), fst.end(), snd.begin(), 0U, plus_bitcount, std::bit_and<uint8_t>());
      double unio_count = std::inner_product(fst.begin(), fst.end(), snd.begin(), 0U, plus_bitcount, std::bit_or<uint8_t>());
      // double diff_count = static_cast<double>(std::count(setdifference.begin(), setdifference.end(), '1'));
      // double unio_count = static_cast<double>(std::count(setunion.begin(), setunion.end(), '1'));
      assert(fstsid != sndsid);
      assert(diff_count <= unio_count);
      //Jaccard distance
      double jaccard = 1 - diff_count / unio_count;
      if(verbose) {
	std::cout << boost::format("\t%6d $%04X %20.15e") % sndsid % sndsid % jaccard;
	std::cout << std::endl;
      }
      distances.push_back(std::make_pair(sndsid, jaccard));
    }
  }
  return distances;
}


void list_entries(pqxx::work &txn, const DistancesVector &distances) {
  std::vector<unsigned int> sids(distances.size());
  std::ostringstream query;
  boost::format format("*%6d L=$%04X %31s %31s %31s %s\n");

  std::transform(distances.begin(), distances.end(), sids.begin(), [](std::pair<unsigned int, double> x) { return x.first; });
  query << "SELECT sid,name,author,released,filename,length(data) FROM songs NATURAL JOIN files WHERE sid ";
  if(sids.size() >=2) {
    query << "IN (";
    auto sids_end(sids.end());
    --sids_end;
    std::copy(sids.begin(), sids_end, std::ostream_iterator<unsigned int>(query, ","));
    query << *sids_end << ") ORDER BY sid;";
  } else {
    query << "= " << sids[0] << ';';
  }
  //std::cout << query.str() << std::endl;
  pqxx::result result(txn.exec(query.str()));
  for(auto row : result) {
    std::cout << format
      % row[0].as<unsigned int>()
      % row[5].as<unsigned int>()
      % row[1].c_str()
      % row[2].c_str()
      % row[3].c_str()
      % row[4]
      ;
  }
}


void reduce_to_lowest(DistancesVector &distances, unsigned int num) {
  auto cmpfun = [](const std::pair<unsigned int, double> &x, const std::pair<unsigned int, double> &y) { return x.second < y.second; };
  if(distances.size() < num) {
    std::sort(distances.begin(), distances.end(), cmpfun);
  } else {
    std::partial_sort(distances.begin(), distances.begin() + num, distances.end(), cmpfun);
    distances.resize(num);
  }
}

unsigned int closer_than(DistancesVector &distances, double delta) {
  unsigned int idx;
  auto cmpfun = [](const std::pair<unsigned int, double> &x, const std::pair<unsigned int, double> &y) { return x.second < y.second; };

  std::sort(distances.begin(), distances.end(), cmpfun);
  for(idx = 0; idx < distances.size(); ++idx) {
    if(distances[idx].second > delta) break;
  }
  if(idx > 0) {
    distances.resize(idx);
  } else {
    distances.clear();
  }
  return idx;
}

int run(pqxx::connection &conn, char **begin, char **end, const gengetopt_args_info &args) {
  try {
    pqxx::work txn(conn, "recall bitshred");
    while(begin < end) {
      unsigned int sid = atoi(*begin);
      std::cout << "SID: " << sid << std::endl;
      auto minsids(calc_distances(txn, sid, args.size_arg, args.ngram_arg, args.hash_arg, args.verbose_flag));
      if(args.closer_given) {
	closer_than(minsids, args.closer_arg);
      } else {
	reduce_to_lowest(minsids, 8);
      }
      //
      if(minsids.empty()) throw std::logic_error("empty minsid");
      auto minsid = minsids.begin();
      std::cout << boost::format("Minimum to %d: %d $%04X d=%20.16e\n") % sid % minsid->first % minsid->first % minsid->second;
      for(auto i : minsids) std::cout << boost::format("|\t %6d $%04X d=%20.16e\n") % i.first % i.first % i.second;
      if(args.query_flag) list_entries(txn, minsids);
      ++begin;
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
    retval = run(conn, &args.inputs[0], &args.inputs[args.inputs_num], args);
  }
  catch (const std::exception &e) {
    std::cerr << "ERROR: " << e.what() << std::endl;
    return retval;
  }
  return retval;

}
