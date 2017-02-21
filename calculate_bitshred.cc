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

/*! Bob Jenkins's hash function
 *
 * https://en.wikipedia.org/wiki/Jenkins_hash_function
 */
uint32_t jenkins_one_at_a_time_hash(const uint8_t* key, size_t length) {
  size_t i = 0;
  uint32_t hash = 0;
  while (i != length) {
    hash += key[i++];
    hash += hash << 10;
    hash ^= hash >> 6;
  }
  hash += hash << 3;
  hash ^= hash >> 11;
  hash += hash << 15;
  return hash;
}

/*! D.J. Bernsteins hash
 *
 * see http://www.cse.yorku.ca/~oz/hash.html
 */
uint32_t djb2_hash(const uint8_t *data, size_t length) {
  size_t i = 0;
  uint32_t hash = 5381;
  
  while (i != length) {
    hash = hash * 33 + data[i++];
  }
  return hash;
}


/*! Alternative D.J. Bernsteins hash
 *
 * see http://www.cse.yorku.ca/~oz/hash.html
 */
uint32_t djb2xor_hash(const uint8_t *data, size_t length) {
  size_t i = 0;
  uint32_t hash = 5381;
  
  while (i != length) {
    hash = hash * 33 ^ data[i++];
  }
  return hash;
}


// SBoxHash by Bret Mulvey [http://papa.bretmulvey.com/post/124028832958/hash-functions-continued].
uint32_t sbox_hash(const uint8_t *data, size_t length) {
  static const uint32_t sbox[] = {
    0xF53E1837, 0x5F14C86B, 0x9EE3964C, 0xFA796D53,  0x32223FC3, 0x4D82BC98, 0xA0C7FA62, 0x63E2C982,
    0x24994A5B, 0x1ECE7BEE, 0x292B38EF, 0xD5CD4E56,  0x514F4303, 0x7BE12B83, 0x7192F195, 0x82DC7300,
    0x084380B4, 0x480B55D3, 0x5F430471, 0x13F75991,  0x3F9CF22C, 0x2FE0907A, 0xFD8E1E69, 0x7B1D5DE8,
    0xD575A85C, 0xAD01C50A, 0x7EE00737, 0x3CE981E8,  0x0E447EFA, 0x23089DD6, 0xB59F149F, 0x13600EC7,
    0xE802C8E6, 0x670921E4, 0x7207EFF0, 0xE74761B0,  0x69035234, 0xBFA40F19, 0xF63651A0, 0x29E64C26,
    0x1F98CCA7, 0xD957007E, 0xE71DDC75, 0x3E729595,  0x7580B7CC, 0xD7FAF60B, 0x92484323, 0xA44113EB,
    0xE4CBDE08, 0x346827C9, 0x3CF32AFA, 0x0B29BCF1,  0x6E29F7DF, 0xB01E71CB, 0x3BFBC0D1, 0x62EDC5B8,
    0xB7DE789A, 0xA4748EC9, 0xE17A4C4F, 0x67E5BD03,  0xF3B33D1A, 0x97D8D3E9, 0x09121BC0, 0x347B2D2C,
    0x79A1913C, 0x504172DE, 0x7F1F8483, 0x13AC3CF6,  0x7A2094DB, 0xC778FA12, 0xADF7469F, 0x21786B7B,
    0x71A445D0, 0xA8896C1B, 0x656F62FB, 0x83A059B3,  0x972DFE6E, 0x4122000C, 0x97D9DA19, 0x17D5947B,
    0xB1AFFD0C, 0x6EF83B97, 0xAF7F780B, 0x4613138A,  0x7C3E73A6, 0xCF15E03D, 0x41576322, 0x672DF292,
    0xB658588D, 0x33EBEFA9, 0x938CBF06, 0x06B67381,  0x07F192C6, 0x2BDA5855, 0x348EE0E8, 0x19DBB6E3,
    0x3222184B, 0xB69D5DBA, 0x7E760B88, 0xAF4D8154,  0x007A51AD, 0x35112500, 0xC9CD2D7D, 0x4F4FB761,
    0x694772E3, 0x694C8351, 0x4A7E3AF5, 0x67D65CE1,  0x9287DE92, 0x2518DB3C, 0x8CB4EC06, 0xD154D38F,
    0xE19A26BB, 0x295EE439, 0xC50A1104, 0x2153C6A7,  0x82366656, 0x0713BC2F, 0x6462215A, 0x21D9BFCE,
    0xBA8EACE6, 0xAE2DF4C1, 0x2A8D5E80, 0x3F7E52D1,  0x29359399, 0xFEA1D19C, 0x18879313, 0x455AFA81,
    0xFADFE838, 0x62609838, 0xD1028839, 0x0736E92F,  0x3BCA22A3, 0x1485B08A, 0x2DA7900B, 0x852C156D,
    0xE8F24803, 0x00078472, 0x13F0D332, 0x2ACFD0CF,  0x5F747F5C, 0x87BB1E2F, 0xA7EFCB63, 0x23F432F0,
    0xE6CE7C5C, 0x1F954EF6, 0xB609C91B, 0x3B4571BF,  0xEED17DC0, 0xE556CDA0, 0xA7846A8D, 0xFF105F94,
    0x52B7CCDE, 0x0E33E801, 0x664455EA, 0xF2C70414,  0x73E7B486, 0x8F830661, 0x8B59E826, 0xBB8AEDCA,
    0xF3D70AB9, 0xD739F2B9, 0x4A04C34A, 0x88D0F089,  0xE02191A2, 0xD89D9C78, 0x192C2749, 0xFC43A78F,
    0x0AAC88CB, 0x9438D42D, 0x9E280F7A, 0x36063802,  0x38E8D018, 0x1C42A9CB, 0x92AAFF6C, 0xA24820C5,
    0x007F077F, 0xCE5BC543, 0x69668D58, 0x10D6FF74,  0xBE00F621, 0x21300BBE, 0x2E9E8F46, 0x5ACEA629,
    0xFA1F86C7, 0x52F206B8, 0x3EDF1A75, 0x6DA8D843,  0xCF719928, 0x73E3891F, 0xB4B95DD6, 0xB2A42D27,
    0xEDA20BBF, 0x1A58DBDF, 0xA449AD03, 0x6DDEF22B,  0x900531E6, 0x3D3BFF35, 0x5B24ABA2, 0x472B3E4C,
    0x387F2D75, 0x4D8DBA36, 0x71CB5641, 0xE3473F3F,  0xF6CD4B7F, 0xBF7D1428, 0x344B64D0, 0xC5CDFCB6,
    0xFE2E0182, 0x2C37A673, 0xDE4EB7A3, 0x63FDC933,  0x01DC4063, 0x611F3571, 0xD167BFAF, 0x4496596F,
    0x3DEE0689, 0xD8704910, 0x7052A114, 0x068C9EC5,  0x75D0E766, 0x4D54CC20, 0xB44ECDE2, 0x4ABC653E,
    0x2C550A21, 0x1A52C0DB, 0xCFED03D0, 0x119BAFE2,  0x876A6133, 0xBC232088, 0x435BA1B2, 0xAE99BBFA,
    0xBB4F08E4, 0xA62B5F49, 0x1DA4B695, 0x336B84DE,  0xDC813D31, 0x00C134FB, 0x397A98E6, 0x151F0E64,
    0xD9EB3E69, 0xD3C7DF60, 0xD2F2C336, 0x2DDD067B,  0xBD122835, 0xB0B3BD3A, 0xB0D54E46, 0x8641F1E4,
    0xA0B38F96, 0x51D39199, 0x37A6AD75, 0xDF84EE41,  0x3C034CBA, 0xACDA62FC, 0x11923B8B, 0x45EF170A,
  };
  uint32_t hash = 0;
  size_t i;

  for (i = 0; i < length; i++) {
    hash ^= sbox[data[i]];
    hash *= 3;
  }
  return hash;
}

/*
 * Other hash functions: see https://www.strchr.com/hash_functions.
 * https://github.com/aappleby/smhasher
 * https://github.com/Cyan4973/xxHash
 * https://github.com/lemire/rollinghashcpp
 * http://www.partow.net/programming/hashfunctions/
 *
 * Libraries: libtlsh-dev
 * libmhash-dev
 *
 * Programs: mhap
 * simhash
 * ssdeep
 */

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


std::bitset<8192> calculate_bitshred(const pqxx::binarystring &data, unsigned int n /*TODO: hash*/) {
  std::bitset<8192> bitshred;
  pqxx::binarystring::const_iterator dbegin(data.begin());
  pqxx::binarystring::const_iterator dend(data.end());

  if(std::distance(dbegin, dend) < n) throw std::invalid_argument("not enough bytes for bitshred");
  for(auto ptr = dbegin; ptr < dend - n; ++ptr) {
    uint32_t hash = jenkins_one_at_a_time_hash(&ptr[0], n);
    bitshred.set(hash % 8192);
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

  query << "INSERT INTO bitshred (sid, m, n, hash, bitshred) VALUES ("
	<< txn.quote(sid) << ", "
	<< txn.quote(m) << ", "
	<< txn.quote(n) << ", "
	<< txn.quote(hashname) << ", "
    //Danger, no quoting as pqxx does not understands this type!
	<< "B'" << bitshred << "');";
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
      //std::vector<unsigned> debug81(i.second.size());
      //std::copy(i.second.begin(), i.second.end(), debug81.begin());
      //std::for_each(debug81.begin(), debug81.end(), [](unsigned x) { std::cout << boost::format("%02x") % x; });
      //std::bitset<8192> bitshred___(calculate_bitshred(i.second, args.ngram_arg));
      BitshredType bitshred(calculate_bitshred(i.second, args.size_arg, args.ngram_arg, hash_function));
      unsigned int bits = store_bitshred(conn, i.first, args.size_arg, args.ngram_arg, args.hash_arg, bitshred);
      //std::cout << bitshred;
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
