### First stage
FROM golang:1.21 as build-root

WORKDIR /build

COPY go.mod .
COPY go.sum .

RUN go mod download

COPY . .

ENV CGO_ENABLED=0
ENV GOOS=linux
ENV GOARCH=amd64

RUN go build -ldflags "-X main.appver=$version -X main.gitref=$git_commit" ./...

## Second stage
FROM scratch

COPY --from=build-root /build/timelord /

ENTRYPOINT ["/timelord"]

EXPOSE 60000

ARG git_commit=unknown
ARG version="2.9.0"
ARG descriptive_version=unknown

LABEL org.cyverse.git-ref="$git_commit"
LABEL org.cyverse.version="$version"
LABEL org.cyverse.descriptive-version="$descriptive_version"
LABEL org.label-schema.vcs-ref="$git_commit"
LABEL org.label-schema.vcs-url="https://github.com/cyverse-de/timelord"
LABEL org.label-schema.version="$descriptive_version"
