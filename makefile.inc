## For students: change this path to the root of your code
CODEROOT = "$(realpath $(dir $(lastword $(MAKEFILE_LIST))))"
#CC = gcc
#CC = g++

UNAME := $(shell uname)
ifeq ($(UNAME),Linux)
     CC = g++
endif

ifeq ($(UNAME),Darwin)
     CC = /opt/local/bin/g++-mp-4.7
     CXX = /opt/local/bin/g++-mp-4.7
endif

#CPPFLAGS = -Wall -I$(CODEROOT) -O3  # maximal optimization
#CPPFLAGS = -std=c++0x -Wall -I$(CODEROOT) -DDATABASE_FOLDER=\"$(CODEROOT)/db\"  -g     # with debugging info
CPPFLAGS = -std=c++0x -Wall -I$(CODEROOT) -DDATABASE_FOLDER=\"$(CODEROOT)/db\" -DDEBUG=true  -g     # with debugging info
