FROM python:3.6-jessie

RUN apt-get update
RUN apt-get install -y sudo
RUN pip install pipenv
