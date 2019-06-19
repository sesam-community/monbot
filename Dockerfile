FROM python:3.7.2
MAINTAINER fluan "feng.luan@hafslundnett.no"

# Install app dependencies
RUN mkdir /monbot
COPY . /monbot
WORKDIR /monbot

RUN pip3 install -r requirements.txt

ENV LOGLEVEL INFO

USER 200

ENTRYPOINT exec python monbot/monbot.py

