## For students: change this path to the root of your code
CODEROOT = $(realpath $(dir $(lastword $(MAKEFILE_LIST))))
//CODEROOT = "/Users/ekinoguz/Dropbox/2013S/CS222/project/project1-65849654/codebase"
#CC = gcc
CC = g++

#CPPFLAGS = -Wall -I$(CODEROOT) -O3  # maximal optimization
CPPFLAGS = -Wall -I$(CODEROOT) -g     # with debugging info
