filedb: filedb.c
	gcc -Wall -O3 -o $@ $^

clean:
	-rm filedb

.PHONY:clean