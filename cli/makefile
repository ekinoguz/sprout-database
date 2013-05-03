
include ../makefile.inc

all: libcli.a clitest

# lib file dependencies
libcli.a: libcli.a(cli.o)  # and possibly other .o files

# c file dependencies
cli.o: cli.h

clitest.o: cli.h

# binary dependencies
clitest: clitest.o libcli.a $(CODEROOT)/rm/librm.a $(CODEROOT)/pf/libpf.a


# dependencies to compile used libraries
.PHONY: $(CODEROOT)/pf/libpf.a $(CODEROOT)/rm/librm.a
$(CODEROOT)/rm/librm.a:
	$(MAKE) -C $(CODEROOT)/rm librm.a

$(CODEROOT)/pf/libpf.a:
	$(MAKE) -C $(CODEROOT)/pf libpf.a


.PHONY: clean
clean:
	-rm clitest *.a *.o *~
	$(MAKE) -C $(CODEROOT)/pf clean
	$(MAKE) -C $(CODEROOT)/rm clean	
