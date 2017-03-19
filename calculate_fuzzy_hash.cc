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
#include <boost/lexical_cast.hpp>
#include "calculate_fuzzy_hash.cmdline.h"

#define RESULT_STRIDE 23
#ifndef MIN_DATA_LENGTH
#define MIN_DATA_LENGTH 256
#endif

class Fuzzy_Interface {
protected:
  struct Comp_Res {
    unsigned int sid;
    double difference;
    bool operator<(const Comp_Res &other) const { return difference < other.difference; }
  };
  typedef std::vector<Comp_Res> ComRes_List;
  virtual ComRes_List calc_differences(pqxx::work &txn, unsigned int sid) = 0;

public:
  /*! \brief List of SID ids
   *
   * It is used whenever a list of SID IDs is needed.
   */
  typedef std::vector<unsigned int> SID_List_Type;

  /*! \brief All missing hashes are calculated
   *
   * This function has to calculate all the missing hashes in the
   * database. It is always called.
   *
   * \param conn database connection object
   * \return number of actually calculated hashes
   */
  virtual unsigned long calculate_missing_hashes(pqxx::connection &conn) = 0;

  /*! \brief Find similar SID files.
   *
   * Similar SID are found used the fuzzy hash in this class. The
   * maximum number is given by args.maximum_dist_arg.
   *
   * \param txn transaction object
   * \param sids list of SIDs to find similar songs to
   * \param args CLI arguments
   */
  virtual void find_similarities(pqxx::work &txn, const SID_List_Type &sids, const gengetopt_args_info &args) {
    for(unsigned int sid : sids) {
      std::cout << "\v\tFinding closest to sid: " << sid << std::endl;
      ComRes_List differences(calc_differences(txn, sid));
      std::sort(differences.begin(), differences.end());
      if(differences.size() > static_cast<unsigned int>(args.maximum_dist_arg)) {
	differences.resize(args.maximum_dist_arg);
      }
      output_differences(txn, differences);
    }
  }

  /*! \brief Nice output.
   *
   * Output the found SIDs with difference measure to stdout.
   *
   * \param txn database transaction object
   * \param differences List of SIDs, type is SID_List_Type
   */
  virtual void output_differences(pqxx::work &txn, const ComRes_List &differences) {
    pqxx::result result;
    
    for(auto i : differences) {
      result = txn.exec((boost::format("SELECT sid,name,author,released,filename,length(data) FROM songs NATURAL JOIN files WHERE sid = %u") % i.sid).str(), "natural join");
      std::cout << boost::format("%5u %8.3e %32s %32s %32s %s\n")
	% i.sid % i.difference
	% result[0]["name"]
	% result[0]["author"]
	% result[0]["released"]
	% result[0]["filename"]
	;
    }
  }

  /*\brief Destructor
   *
   * Needed for working virtual functions.
   */
  virtual ~Fuzzy_Interface() {}
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
  std::string retrieve_hash(pqxx::work &txn, unsigned int sid) {
    std::ostringstream query, hs;
    std::string hash;
 
    query << "SELECT blocksize, hash FROM fuzzy_ssdeep WHERE"
	  << " sid = " << sid
	  << ";";
    pqxx::result result(txn.exec(query.str()));
    if(result.empty()) throw std::runtime_error("can not retrieve hash");
    hs << result[0]["blocksize"].as<unsigned int>()
       << ':'
       << result[0]["hash"].as<std::string>();
    hash = hs.str();
    assert(hash.size() <= FUZZY_MAX_RESULT);
    return hash;
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

  ComRes_List calc_differences(pqxx::work &txn, unsigned int sid) {
    pqxx::result result;
    ComRes_List distvec;
    std::ostringstream query;
    double diff;
 
    std::string left(retrieve_hash(txn, sid));
    query << "SELECT sid, blocksize, hash FROM fuzzy_ssdeep;";
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
	distvec.push_back({rsid, diff});
      }
    }
    return distvec;
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
  
public:
  ComRes_List calc_differences(pqxx::work &txn, unsigned int sid) {
    pqxx::result result;
    Tlsh left, right;
    ComRes_List distvec;

    result = txn.exec((boost::format("SELECT encode(hash, 'hex') AS h FROM fuzzy_tlsh WHERE sid = %u;") % sid).str());
    left.fromTlshStr(result[0]["h"].c_str());
    pqxx::icursorstream cursor(txn, "SELECT sid, encode(hash, 'hex') AS h FROM fuzzy_tlsh", "cursor for TLSH", RESULT_STRIDE);
    while(cursor >> result) {
      for(auto row : result) {
	right.fromTlshStr(row["h"].c_str());
	double diff = left.totalDiff(&right);
	//std::cout << row["sid"].c_str() << ' ' << row["h"].c_str() << ' ' << diff <<std::endl;
	distvec.push_back({row["sid"].as<unsigned int>(), diff});
      }
    }
    return distvec;
  }

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
      Fuzzy_Interface::SID_List_Type sids(std::distance(begin, end));
      std::transform(begin, end, sids.begin(), [](const char *arg) { return boost::lexical_cast<unsigned int>(arg); });
      fuzzy_interface->find_similarities(txn, sids, args);
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
