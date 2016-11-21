FROM andrewosh/binder-base

USER root

RUN apt-get update
RUN apt-get install -y libgeos-c1 && apt-get clean

USER main

ADD requirements.txt /home/main/
RUN pip install -r requirements.txt

