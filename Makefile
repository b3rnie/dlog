# -*- mode: Makefile -*-

suite = $(if $(SUITE), suite=$(SUITE), )

.PHONY:	all deps check compile rel dist xref test clean distclean

all:	deps compile

deps:
	./rebar get-deps
	git submodule update --init

compile:
	./rebar compile

rel:	all
	./rebar generate

dist:	rel
	tar -czf dlog.tar.gz rel/dlog

xref:
	./rebar xref

test:
	./rebar eunit $(suite) skip_deps=true

clean:
	./rebar clean

distclean:	clean
	./rebar delete-deps
	rm -rf rel/dlog
	rm dlog.tar.gz

#eof
