
FROM debian:jessie
MAINTAINER kev <noreply@easypi.pro>

LABEL Name=docker_scrapper Version=0.0.1 
RUN apt-get update \
    && apt-get install -y build-essential checkinstall wget tar git \
    && apt-get install -y libreadline-gplv2-dev libncursesw5-dev libssl-dev libsqlite3-dev tk-dev libgdbm-dev libc6-dev libbz2-dev
RUN wget https://www.python.org/ftp/python/3.6.2/Python-3.6.2.tgz
RUN tar xzf Python-3.6.2.tgz
RUN cd Python-3.6.2
RUN ./configure
RUN make altinstall

RUN rm -rf Python-3.6.2
RUN rm -rf Python-3.6.2.tgz

RUN pip3.6 -V
RUN pip3.6 install numpy pandas scrapy w3lib peewee 

RUN cd /workdir && git clone https://github.com/XetRAHF/Spider_kvk.nl.git spider \
    && cd /workdir/spider/ 

CMD cd /workdir/spider/ && bash start_spider.sh
