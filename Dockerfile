FROM golang:1.16.4-alpine3.13 AS build

RUN mkdir /src
WORKDIR /src

# Get required Go modules first to cache them if nothing changed
COPY go.mod .
COPY go.sum .
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -a -o /go/bin/cloudsqltail /src/cmd/cloudsqltail

# runtime container
FROM alpine:3.13

RUN apk add --update ca-certificates
# honeytail was compiled against libc, not musl, but they're compatible
RUN mkdir /lib64 && ln -s /lib/libc.musl-x86_64.so.1 /lib64/ld-linux-x86-64.so.2

RUN mkdir /app
WORKDIR /app
COPY --from=build /go/bin/cloudsqltail ./cloudsqltail
COPY run.sh .
RUN wget -q -O honeytail https://honeycomb.io/download/honeytail/linux/1.762 \
    && echo '00e24441316c7ae24665b1aaea4cbb77e2ee52c83397bf67d70f3ffe14a1e341  honeytail' | sha256sum -c \
    && chmod 755 /app/honeytail
CMD ["ash", "/app/run.sh"]
