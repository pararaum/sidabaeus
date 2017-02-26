#! /usr/bin/make

CXXFLAGS = -O2 -Wall -Wextra -std=c++11 -DNDEBUG
LIBS = -lpqxx

EXES = calc_bigram_distances test_data_types calculate_bitshred find_closest_bitshred calculate_fuzzy_hash

all:	$(EXES)

calc_bigram_distances: calc_bigram_distances.o
	$(CXX) -g -o $@ $+ $(LIBS)

test_data_types: test_data_types.o
	$(CXX) -g -o $@ $+ $(LIBS)

calculate_bitshred.cmdline.h: calculate_bitshred.ggo
	gengetopt --conf-parser -F calculate_bitshred.cmdline < $<

calculate_bitshred.cmdline.o: calculate_bitshred.cmdline.c calculate_bitshred.ggo

calculate_bitshred: calculate_bitshred.cmdline.h calculate_bitshred.cmdline.o calculate_bitshred.o
	$(CXX) -g -o $@ $+ $(LIBS)

calculate_fuzzy_hash.cmdline.h: calculate_fuzzy_hash.ggo
	gengetopt --unamed-opts --conf-parser -F calculate_fuzzy_hash.cmdline < $<

calculate_fuzzy_hash: calculate_fuzzy_hash.cmdline.h calculate_fuzzy_hash.cmdline.o calculate_fuzzy_hash.o
	$(CXX) -g -o $@ $+ -ltlsh $(LIBS) -lfuzzy

#find_closest_bitshred8192: find_closest_bitshred8192.o
#	$(CXX) -g -o $@ $+ $(LIBS)

find_closest_bitshred.cmdline.o: find_closest_bitshred.cmdline.c find_closest_bitshred.ggo

find_closest_bitshred.cmdline.c: find_closest_bitshred.ggo
	gengetopt --unamed-opts --conf-parser -F find_closest_bitshred.cmdline < $<

find_closest_bitshred: find_closest_bitshred.cmdline.o find_closest_bitshred.o
	$(CXX) -g -o $@ $+ $(LIBS)

.PHONY: clean
clean:
	rm -f *.o
	rm -f $(EXES)

.PHONY: distclean
distclean: clean
	rm *.cmdline.c *.cmdline.h

