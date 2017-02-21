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

struct option long_options[] {
  { "dbname", required_argument, 0, 'd' },
  { "dbhost", required_argument, 0, 'H' },
  { "dbpass", required_argument, 0, 'p' },
  { "dbuser", required_argument, 0, 'u' },
  { 0, 0, 0, 0}
};


int run(pqxx::connection &conn) {
  pqxx::work txn(conn, "test data types");
  txn.exec("CREATE TEMP TABLE test (data bytea);");
  txn.exec("INSERT INTO test VALUES(E'test\\\\000....'::bytea);");
  txn.exec("CREATE TEMP TABLE bits (data BIT VARYING(17));");
  txn.exec("INSERT INTO bits (data) VALUES(B'10000110111110001');");
  txn.exec("INSERT INTO bits (data) VALUES(B'00101010101010101');");
  //SELECT b1.data,b2.data,b1.data&b2.data,b1.data#b2.data FROM bits b1 CROSS JOIN bits b2;
  //txn.exec("");

  try {
    pqxx::result query1(txn.exec("SELECT data FROM test;"));
    for(auto i : query1) {
      std::cout << i[0].size() << '\t' << i[0] << std::endl;
      pqxx::binarystring binstr(i[0]);
      std::cout << binstr.str() << std::endl;
      std::for_each(binstr.begin(), binstr.end(), [](pqxx::binarystring::value_type x) {
	  std::cout << boost::format("%04X %4d\n") % (int)x % (int)x;
	});
    }
    pqxx::result query2(txn.exec("SELECT data FROM bits;"));
    for(const pqxx::tuple &i : query2) {
      const pqxx::result::field &col1(i["data"]);
      std::cout << col1 << std::endl;
      //First get the string as field is not accessible.
      //error error: ‘from_string’ is not a member of ‘pqxx::string_traits<std::vector<bool> >’: bitfield = col1.as<std::vector<bool> >();
      //error: ‘from_string’ is not a member of ‘pqxx::string_traits<std::vector<bool> >’: i["data"].to(bitfield);
      std::string bitfieldstr;
      std::vector<bool> bitfield(col1.size());
      col1.to(bitfieldstr);
      std::bitset<17> bitset(bitfieldstr);
      std::transform(bitfieldstr.begin(), bitfieldstr.end(), bitfield.begin(), [](char x) { return x != '0'; });
      std::for_each(bitfield.begin(), bitfield.end(), [](bool x) { std::cout << (x ? '#' : '.'); });
      std::cout << '\t' << ~bitset << std::endl;
    }
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
  int clichar;
  int option_index = 0;
  int retval = -1;
  
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
    retval = run(conn);
  }
  catch (const std::exception &e) {
    std::cerr << "ERROR: " << e.what() << std::endl;
    return retval;
  }
  return retval;
}
