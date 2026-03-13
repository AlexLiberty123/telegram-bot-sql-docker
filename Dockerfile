FROM alpine:3.18 AS builder

RUN apk add --no-cache gcc musl-dev make libpq-dev curl-dev cjson-dev

WORKDIR /app
COPY Makefile bot.c db_init.c ./

RUN make

FROM alpine:3.18

RUN apk add --no-cache libpq libcurl cjson

WORKDIR /app
COPY --from=builder /app/bot /app/bot
COPY --from=builder /app/db_init /app/db_init
COPY videos.json .

CMD sh -c "./db_init && ./bot"