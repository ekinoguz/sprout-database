
include ../makefile.inc

all: libcli.a clitest start

# lib file dependencies
libcli.a: libcli.a(cli.o)  # and possibly other .o files

# c file dependencies
cli.o: cli.h

clitest.o: cli.h

start.o: cli.h

# binary dependencies
clitest: clitest.o libcli.a $(CODEROOT)/qe/libqe.a $(CODEROOT)/ix/libix.a $(CODEROOT)/rm/librm.a $(CODEROOT)/pf/libpf.a
start: start.o libcli.a $(CODEROOT)/qe/libqe.a $(CODEROOT)/ix/libix.a $(CODEROOT)/rm/librm.a $(CODEROOT)/pf/libpf.a

$(CODEROOT)/rm/librm.a:
	$(MAKE) -C $(CODEROOT)/rm librm.a

$(CODEROOT)/pf/libpf.a:
	$(MAKE) -C $(CODEROOT)/pf libpf.a

# dependencies to compile used libraries
.PHONY: $(CODEROOT)/pf/libpf.a $(CODEROOT)/rm/librm.a
$(CODEROOT)/ix/libix.a:
	$(MAKE) -C $(CODEROOT)/ix libix.a


.PHONY: $(CODEROOT)/pf/libpf.a $(CODEROOT)/rm/librm.a $(CODEROOT)/ix/libix.a
$(CODEROOT)/qe/libqe.a:
	$(MAKE) -C $(CODEROOT)/qe libqe.a

.PHONY: clean
clean:
	-rm clitest start *.a *.o *~
	$(MAKE) -C $(CODEROOT)/pf clean
	$(MAKE) -C $(CODEROOT)/rm clean
	$(MAKE) -C $(CODEROOT)/ix clean	
	$(MAKE) -C $(CODEROOT)/qe clean	
