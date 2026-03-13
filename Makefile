CC=gcc
CFLAGS=-Wall -O2 -I/usr/include/postgresql
LDFLAGS=-lpq -lcurl -lcjson

all: db_init bot

db_init: db_init.c
	$(CC) $(CFLAGS) -o db_init db_init.c $(LDFLAGS)

bot: bot.c
	$(CC) $(CFLAGS) -o bot bot.c $(LDFLAGS)

clean:
	rm -f db_init bot