## For students: change this path to the root of your code
CODEROOT = "$(realpath $(dir $(lastword $(MAKEFILE_LIST))))"
#CC = gcc
CC = g++

#CPPFLAGS = -Wall -I$(CODEROOT) -O3  # maximal optimization
CPPFLAGS = -std=c++0x -Wall -I$(CODEROOT) -DDATABASE_FOLDER=\"$(CODEROOT)/db\"  -g     # with debugging info
