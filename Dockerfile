#FROM andrewosh/binder-base
FROM python:3.5-slim

USER root

RUN apt-get update
RUN apt-get install -y libgeos-c1 build-essential python3-dev && apt-get clean

RUN useradd -ms /bin/bash main

WORKDIR /home/main

ADD requirements.txt /home/main/
RUN pip3 install jupyter
RUN pip3 install -r requirements.txt

USER main
