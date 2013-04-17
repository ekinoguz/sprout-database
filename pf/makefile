
include ../makefile.inc

all: libpf.a pftest

# lib file dependencies
libpf.a: libpf.a(pf.o cache.o)  # and possibly other .o files

# c file dependencies
pf.o: pf.h cache.h
cache.o: cache.h

pftest.o: pf.h cache.h

# binary dependencies
pftest: pftest.o libpf.a $(CODEROOT)/pf/libpf.a

# dependencies to compile used libraries
.PHONY: $(CODEROOT)/pf/libpf.a
$(CODEROOT)/pf/libpf.a:
	$(MAKE) -C $(CODEROOT)/pf libpf.a

.PHONY: clean
clean:
	-rm pftest *.a *.o *~
