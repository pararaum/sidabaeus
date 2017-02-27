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
#include <fuzzy.h>
//#include <edit_dist.h>
extern "C" int edit_distn(const char *s1, size_t s1len, const char *s2, size_t s2len);
#include <boost/lexical_cast.hpp>
#include "calculate_fuzzy_hash.cmdline.h"

#define RESULT_STRIDE 23

class Fuzzy_Interface {
public:
  typedef std::vector<unsigned int> SID_List_Type;
  virtual unsigned long calculate_missing_hashes(pqxx::connection &conn) = 0;
  virtual void find_similarities(pqxx::work &txn, const std::vector<unsigned int> &sids, unsigned int maximum_n) = 0;
  // virtual void calculate(const uint8_t *buf, unsigned long size) = 0;
  // virtual void calculate(const pqxx::binarystring &binstr) {
  //   calculate(binstr.data(), binstr.size());
  // }
};


class SSDeep : public Fuzzy_Interface {
protected:
  std::pair<unsigned int, std::string> calculate_hash(const uint8_t *buf, unsigned long size) {
    char hbuf[FUZZY_MAX_RESULT + 1];
    std::string hash_string;
    std::string hash;
    unsigned int blocksize;
    
    if(fuzzy_hash_buf(buf, size, hbuf) != 0) {
      throw std::runtime_error("fuzzy hashing (ssdeep) failed");
    }
    hash_string = hbuf;
    hash = hash_string.substr(hash_string.find(':') + 1);
    hbuf[hash_string.find(':')] = '\0';
    std::istringstream lexical(hbuf);
    if(!(lexical >> blocksize)) throw std::runtime_error("blocksize extraction from ssdeep failed");
    return std::make_pair(blocksize, hash);
  }
  std::pair<unsigned int, std::string> retrieve_hash(pqxx::work &txn, unsigned int sid) {
    std::ostringstream query;
    query << "SELECT blocksize, hash FROM fuzzy_ssdeep WHERE"
	  << " sid = " << sid
	  << ";";
    pqxx::result result(txn.exec(query.str()));
    if(result.empty()) throw std::runtime_error("can not retrieve hash");
    return std::make_pair(result[0]["blocksize"].as<unsigned int>(), result[0]["hash"].as<std::string>());
  }


  void insert(pqxx::work &txn, unsigned int sid, unsigned int blocksize, const std::string &hash) {
    std::ostringstream query;
    query << "INSERT INTO fuzzy_ssdeep (sid, blocksize, hash) VALUES ("
	  << sid << ','
	  << blocksize << ','
	  << txn.quote(hash)
	  << ");";
    txn.exec(query.str());
  }

public:
  virtual unsigned long calculate_missing_hashes(pqxx::connection &conn) {
    std::ostringstream query;
    pqxx::result result;
    unsigned long count = 0;

    query << "SELECT sid, data, length(data) FROM files WHERE sid NOT IN"
	  << " (SELECT sid FROM fuzzy_ssdeep) AND data NOTNULL"
	  << " LIMIT " << RESULT_STRIDE
	  << ';';
    do {
      //Cursor needs nested transactions!?
      //pqxx::icursorstream cursor(txn, query.str(), "cursor for TLSH", RESULT_STRIDE);
      pqxx::work txn(conn, "calculate ssdeep hashes");
      result = txn.exec(query.str());
      for(auto row : result) {
	unsigned long dsize = row[2].as<unsigned long>();
	unsigned long sid = row[0].as<unsigned long>();
	std::cout << boost::format("$%06lx $%04lX\n") % sid % dsize;
	pqxx::binarystring binstr(row["data"]);
	std::pair<unsigned int, std::string> hash(calculate_hash(binstr.data(), binstr.size()));
	std::cout << '\t' << hash.first << "⁚" << hash.second << std::endl;
	insert(txn, sid, hash.first, hash.second);
      }
      txn.commit();
      ++count;
    } while(!result.empty());
    return count - 1;
  }

  void find_similarity(pqxx::work &txn, unsigned int blocksize, const std::string &hash, unsigned int maximum_n) {
    pqxx::result result;
    std::vector<std::pair<unsigned int, int> > distvec;
    std::ostringstream query;
    std::string left;
    int diff;

    query << "SELECT sid, blocksize, hash FROM fuzzy_ssdeep;"; // WHERE blocksize = " << blocksize << ";";
    {
      std::ostringstream left_stream;
      left_stream << blocksize << ':' << hash;
      left = left_stream.str();
    }
    pqxx::icursorstream cursor(txn, query.str(), "cursor for ssdeep", RESULT_STRIDE);
    while(cursor >> result) {
      for(auto row : result) {
	unsigned int rsid = row["sid"].as<unsigned int>();
	unsigned int rblocksize = row["blocksize"].as<unsigned int>();
	std::string  rhash(row["hash"].c_str());
	{
	  std::ostringstream right;
	  right << rblocksize << ':' << rhash;
	  diff = 100 - fuzzy_compare(left.c_str(), right.str().c_str());
	}
	distvec.push_back(std::make_pair(rsid, diff));
      }
    }
    std::sort(distvec.begin(), distvec.end(), [](const std::pair<unsigned int, int> &x, const std::pair<unsigned int, int> &y) { return x.second < y.second; });
    if(distvec.size() > maximum_n) distvec.resize(maximum_n);
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
  virtual void find_similarities(pqxx::work &txn, const std::vector<unsigned int> &sids, unsigned int maximum_n) {
    if(!sids.empty()) {
      std::ostringstream search;
      search << "SELECT sid, blocksize, hash FROM fuzzy_ssdeep WHERE sid in (" << sids[0];
      std::for_each(++sids.begin(), sids.end(), [&search](unsigned int sid) { search << ',' << sid; });
      search << ");";
      std::cout << search.str() << std::endl;
      pqxx::result searchresult(txn.exec(search.str()));
      for(auto row : searchresult) {
	unsigned long sid = row["sid"].as<unsigned long>();
	unsigned long blocksize = row["blocksize"].as<unsigned long>();
	std::string hash(row["hash"].c_str());
	std::cout << boost::format("%6ld '%s'\n") % sid % hash;
	find_similarity(txn, blocksize, hash, maximum_n);
      }
    }
  }
};


class TLSH : public Fuzzy_Interface {
  std::string hash;
protected:
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

public:
  virtual unsigned long calculate_missing_hashes(pqxx::connection &conn) {
    std::ostringstream query;
    pqxx::result result;
    unsigned long count = 0;

    query << "SELECT sid, data, length(data) FROM files WHERE sid NOT IN"
	  << " (SELECT sid FROM fuzzy_tlsh) AND data NOTNULL AND length(data) >= " << MIN_DATA_LENGTH
	  << " LIMIT " << RESULT_STRIDE
	  << ';';
    do {
      //Cursor needs nested transactions!?
      //pqxx::icursorstream cursor(txn, query.str(), "cursor for TLSH", RESULT_STRIDE);
      pqxx::work txn(conn, "calculate TLSH hashes");
      result = txn.exec(query.str());
      for(auto row : result) {
	unsigned long dsize = row[2].as<unsigned long>();
	unsigned long sid = row[0].as<unsigned long>();
	std::cout << boost::format("$%06lx $%04lX\n") % sid % dsize;
	if(dsize >= MIN_DATA_LENGTH) {
	  pqxx::binarystring binstr(row["data"]);
	  std::string hash(calculate_tlsh(binstr));
	  std::cout << '\t' << hash << std::endl;
	  insert_tlsh(txn, sid, hash);
	}
      }
      txn.commit();
      ++count;
    } while(!result.empty());
    return count - 1;
  }
  virtual void find_similarities(pqxx::work &txn, const std::vector<unsigned int> &sids, unsigned int maximum_n) {
    if(!sids.empty()) {
      std::ostringstream search;
      search << "SELECT sid, encode(hash, 'hex') FROM fuzzy_tlsh WHERE sid in (" << sids[0];
      std::for_each(++sids.begin(), sids.end(), [&search](unsigned int sid) { search << ',' << sid; });
      search << ");";
      std::cout << search.str() << std::endl;
      pqxx::result searchresult(txn.exec(search.str()));
      for(auto row : searchresult) {
	unsigned long sid = row["sid"].as<unsigned long>();
	std::string hash(row[1].c_str());
	std::cout << boost::format("%6ld '%s'\n") % sid % hash;
	find_similar_tlsh(txn, hash, maximum_n);
      }
    }
  }
};


int run(pqxx::connection &conn, char **begin, char **end, const gengetopt_args_info &args) {
  std::string hash_type(args.hash_arg);
  Fuzzy_Interface *fuzzy_interface = NULL;

  if(hash_type == "tlsh") {
    fuzzy_interface = new TLSH;
  } else if(hash_type == "ssdeep") {
    fuzzy_interface = new SSDeep;
  } else {
    throw std::runtime_error("unknown hash type: " + hash_type);
  }
  try {
    fuzzy_interface->calculate_missing_hashes(conn);
    //And direct query
    if(begin < end) {
      pqxx::work txn(conn, "query fuzzy hashes");
      std::vector<unsigned int> sids(std::distance(begin, end));
      std::transform(begin, end, sids.begin(), [](const char *arg) { return boost::lexical_cast<unsigned int>(arg); });
      fuzzy_interface->find_similarities(txn, sids, args.maximum_dist_arg);
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
