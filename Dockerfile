## First stage
FROM golang:1.26 AS build-root

WORKDIR /build

COPY go.mod .
COPY go.sum .

RUN go mod download

COPY . .

ENV CGO_ENABLED=0
ENV GOOS=linux
ENV GOARCH=amd64

RUN go build ./...

## Second stage
FROM gcr.io/distroless/static-debian12:nonroot

WORKDIR /

COPY --from=build-root /build/timelord /

ENTRYPOINT ["/timelord"]

EXPOSE 60000
