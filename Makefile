# -*- mode: Makefile -*-

suite = $(if $(SUITE), suite=$(SUITE), )

.PHONY:	all deps check test clean

all:	deps compile

deps:
	./rebar get-deps

compile:
	./rebar compile

rel:	deps
	./rebar compile generate

docs:
	./rebar doc

check:
	./rebar check-plt
	./rebar dialyze

test:
	./rebar eunit $(suite) skip_deps=true


relclean:
	rm -rf rel/paxos

conf_clean:
	@:

clean:
	./rebar clean
	$(RM) doc/*
	$(RM) ebin/.d

#eof
