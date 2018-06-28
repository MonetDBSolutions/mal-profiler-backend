FROM python:alpine3.6

RUN apk update
RUN apk add ca-certificates
RUN update-ca-certificates
