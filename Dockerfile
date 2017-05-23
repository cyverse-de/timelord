FROM discoenv/golang-base:master

ENV CONF_TEMPLATE=/go/src/github.com/cyverse-de/timelord/timelord.yml.tmpl
ENV CONF_FILENAME=timelord.yml
ENV PROGRAM=timelord

ARG git_commit=unknown
ARG version="2.9.0"
ARG descriptive_version=unknown

LABEL org.cyverse.git-ref="$git_commit"
LABEL org.cyverse.version="$version"
LABEL org.cyverse.descriptive-version="$descriptive_version"

COPY . /go/src/github.com/cyverse-de/timelord
RUN go install -v -ldflags "-X main.appver=$version -X main.gitref=$git_commit" github.com/cyverse-de/timelord

EXPOSE 60000
LABEL org.label-schema.vcs-ref="$git_commit"
LABEL org.label-schema.vcs-url="https://github.com/cyverse-de/timelord"
LABEL org.label-schema.version="$descriptive_version"
