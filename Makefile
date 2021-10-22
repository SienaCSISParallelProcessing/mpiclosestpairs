# Makefile for C program to read and process a TMG file

PROGRAM=mpiclosest
CFILES=tmggraph.c stringfuncs.c sll.c $(PROGRAM).c
OFILES=$(CFILES:.c=.o)
CC=mpicc
CFLAGS=-g

$(PROGRAM):	$(OFILES)
	$(CC) $(CFLAGS) -o $(PROGRAM) $(OFILES) -lreadline -lm

clean::
	/bin/rm $(PROGRAM) $(OFILES)
