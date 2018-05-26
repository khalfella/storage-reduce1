CC=gcc

IDIR=jansson/jansson-2.11/root/include
LDIR=jansson/jansson-2.11/root/lib
LIBS= -lavl -lcmdutils -ljansson
CFLAGS=-I$(IDIR) -g -ggdb3 -m64 -Wall -Wextra

PRG = storage-reduce1
OBJ = $(PRG).o

%.o: %.c
	$(CC) -c -o $@ $< $(CFLAGS)

$(PRG): $(OBJ)
	$(CC) -o $@ $^ $(CFLAGS) -L$(LDIR) $(LIBS)

all: $(PRG)
.PHONY: clean
clean:
	rm -f $(PRG) *.o
