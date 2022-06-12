LIBS = -luuid
INC = -I./include
FLAGS = -Wall -O3
SRC = $(wildcard src/*.c)
OBJ = $(patsubst src/%.c,obj/%.o,$(SRC))

CC = gcc
BIN = bench_string bench_int bench_bytes

all:obj $(BIN)

obj:
	@mkdir -p $@

bench_string:bench_string.o db.o
	$(CC) -o $@ $^ $(LIBS)

bench_int:bench_int.o db.o
	$(CC) -o $@ $^ $(LIBS)

bench_bytes:bench_bytes.o db.o
	$(CC) -o $@ $^ $(LIBS)

$(OBJ):obj/%.o:src/%.c
	$(CC) -c $(FLAGS) -o $@ $< $(INC)

clean:
	-rm $(BIN) $(OBJ)
	@rmdir obj

.PHONY:all clean

vpath %.c src
vpath %.o obj
vpath %.h include
