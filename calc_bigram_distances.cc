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

typedef std::vector<unsigned long> SIDs_Container;
typedef std::set<std::pair<unsigned long, unsigned long> > SIDs_Pairs;

struct option long_options[] {
  { "dbname", required_argument, 0, 'd' },
  { "dbhost", required_argument, 0, 'H' },
  { "dbpass", required_argument, 0, 'p' },
  { "dbuser", required_argument, 0, 'u' },
  { "min", required_argument, 0, 'm' },
  { "max", required_argument, 0, 'M' },
  { 0, 0, 0, 0}
};

SIDs_Pairs get_known_sid_distances(pqxx::work &txn) {
  pqxx::result query(txn.exec("SELECT DISTINCT ON (fst, snd) fst, snd FROM bigram_counts_distance;"));
  std::set<std::pair<unsigned long, unsigned long> > known_sid_pairs;
  std::for_each(query.begin(), query.end(), [&known_sid_pairs](const pqxx::result::tuple &x) {
      known_sid_pairs.insert(std::make_pair(x[0].as<unsigned long>(), x[1].as<unsigned long>()));
    });
  return known_sid_pairs;
}

SIDs_Container get_sids_with_counts(pqxx::connection &conn) {
  SIDs_Container sidlist;
  auto fun([&sidlist](const pqxx::result::tuple &x) { sidlist.push_back(x["sid"].as<unsigned long>()); });
  pqxx::work txn(conn, "get_sids_with_counts");
  pqxx::result query(txn.exec("SELECT DISTINCT ON (sid) sid FROM bigram_counts ORDER BY sid;"));

  std::cout << "Query returned " << query.size() << " distinct storage ids.\n";
  std::for_each(query.begin(), query.end(), fun);
  return sidlist;
}



SIDs_Pairs get_sids_without_counts(pqxx::connection &conn) {
  SIDs_Container sidlist;
  auto fun([&sidlist](const pqxx::result::tuple &x) { sidlist.push_back(x["sid"].as<unsigned long>()); });
  //auto fun([&sidlist](const pqxx::result::tuple &x) { sidlist.insert(x[0].as<unsigned long>()); });
  pqxx::work txn(conn, "get_sids_with_counts");
  pqxx::result query(txn.exec("SELECT DISTINCT ON (sid) sid FROM bigram_counts ORDER BY sid;"));

  std::cout << "Query returned " << query.size() << " distinct storage ids.\n";
  std::for_each(query.begin(), query.end(), fun);
  //std::copy(sidlist.begin(), sidlist.end(), std::ostream_iterator<unsigned long>(std::cout, "\t"));
  //std::cout << std::endl;
  //
  std::set<std::pair<unsigned long, unsigned long> > sid_pairs;
  auto sidlist_end(sidlist.end());
  for(auto i = sidlist.begin(); i != sidlist_end; ++i) {
    auto j(i);
    std::for_each(++j, sidlist_end, [i,&sid_pairs](unsigned long x) {
	//std::cout << *i << '\t' << x << std::endl;
	sid_pairs.insert(std::make_pair(*i, x));
      });
  }
  std::cout << "Total number of sid pairs: " << sid_pairs.size() << std::endl;
  std::set<std::pair<unsigned long, unsigned long> > known_sid_pairs(get_known_sid_distances(txn));
  std::cout << "Number of known sid pairs: " << known_sid_pairs.size() << std::endl;
  for(auto i : known_sid_pairs) sid_pairs.erase(i);
  return sid_pairs;
}

void get_2d_histogram(pqxx::work &txn, unsigned long sid, std::array<double,65536> &histo) {
  unsigned long maxc = 0;
  pqxx::result query(txn.exec("SELECT fst,snd,count FROM bigram_counts WHERE sid=" + txn.quote(sid)));

  for(const pqxx::result::tuple &r : query) {
    unsigned long count = r[2].as<unsigned long>();
    histo.at(r[0].as<unsigned>() * 256 + r[1].as<unsigned>()) = count;
    //std::cout << r[0].as<unsigned>() << ',' << r[1].as<unsigned>() << '=' << count << '\n';
    if(count > maxc) maxc = count;
  }
  for(auto &i : histo) i /= maxc;
}

double calc_distance(const std::array<double,65536> &left, const std::array<double,65536> right) {
  std::array<double,65536> temp;
  std::transform(left.begin(), left.end(), right.begin(), temp.begin(), [](double x, double y) {
      double d = x - y;
      return d*d;
    });
  return std::sqrt(std::accumulate(temp.begin(), temp.end(), 0.0));
}


void insert_distance(pqxx::work &txn, unsigned long first, unsigned long second, double distance) {
  txn.exec("INSERT INTO bigram_counts_distance"
	   " (fst,snd,distance)"
	   " VALUES(" + txn.quote(first) + ',' + txn.quote(second) + ',' + txn.quote(distance) + ')');
}


int run(pqxx::connection &conn, unsigned long int min, unsigned long int max) {
 std::array<double,65536> histo1, histo2;
 auto sidlist(get_sids_with_counts(conn));

 if(min > 0) sidlist.erase(std::remove_if(sidlist.begin(), sidlist.end(), [min] (unsigned long x) { return x < min; }), sidlist.end());
 if(max > 0) sidlist.erase(std::remove_if(sidlist.begin(), sidlist.end(), [max] (unsigned long x) { return x > max; }), sidlist.end());
 auto sidlist_end(sidlist.end());
 for(auto i = sidlist.begin(); i != sidlist_end; ++i) {
   auto j(i);
   try {
     histo1.fill(0);
     pqxx::work txn(conn, "sidlist outer loop");
     get_2d_histogram(txn, *i, histo1);
     std::for_each(++j, sidlist_end, [i, &txn, &conn, &histo1, &histo2](unsigned long x) {
	 histo2.fill(0);
	 get_2d_histogram(txn, x, histo2);
	 double distance = calc_distance(histo1, histo2);
	 std::cout << boost::format("Inserting (%08X,%08X) d=%12.6e\n") % *i % x % distance;
	 insert_distance(txn, *i, x, distance);
       });
     txn.commit();
   }
   catch(const std::exception &excp) {
     std::cerr << "Exception: " << excp.what() << std::endl;
   }
 }
 return 0;
}

int main(int argc, char **argv) {
  std::ostringstream connection_string;
  std::string dbname("chip");
  std::string dbuser("chip");
  std::string dbpass;
  std::string dbhost;
  int clichar;
  int option_index = 0;
  int retval = -1;
  int min = -1;
  int max = -1;
  
  if(std::getenv("SIDDB")) dbname = std::getenv("SIDDB");
  if(std::getenv("SIDUSER")) dbname = std::getenv("SIDUSER");
  while((clichar = getopt_long(argc, argv, "h", long_options, &option_index)) != -1) {
    switch(clichar) {
    case 0:
      /* long only */
      break;
    case 'd':
      dbname = optarg;
      break;
    case 'H':
      dbhost = optarg;
      break;
    case 'p':
      dbpass = optarg;
      break;
    case 'u':
      dbuser = optarg;
      break;
    case 'h':
      std::cerr << "Usage: calc_bigram_distances ...\n";
      return 1;
    case 'm':
      min = std::atoi(optarg);
      break;
    case 'M':
      max = std::atoi(optarg);
      break;
    default:
      std::cerr << "Unknow getopt return code " << clichar << std::endl;
      return -1;
    }
  }
  try {
    connection_string << "dbname=" << dbname << " user=" << dbuser;
    if(!dbhost.empty()) connection_string << " host=" << dbhost;
    if(dbpass.size() > 0) connection_string << " password=" << dbpass;
    pqxx::connection conn(connection_string.str());
    retval = run(conn, min, max);
  }
  catch (const std::exception &e) {
    std::cerr << "ERROR: " << e.what() << std::endl;
    return retval;
  }
  return retval;
}
