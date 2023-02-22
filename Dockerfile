FROM python:3.11-buster

WORKDIR /birdwatch-parsing

COPY requirements.txt requirements.txt

RUN apt update && apt install -y gcc g++

RUN pip3 install --upgrade pip
RUN python3 -m pip install --upgrade setuptools
RUN pip3 install -r requirements.txt

COPY import-tsv.py import-tsv.py
COPY ./keys/credentials.json credentials.json
COPY .env .env

ENV GOOGLE_APPLICATION_CREDENTIALS credentials.json

CMD [ "python3", "-u", "import-tsv.py" ]