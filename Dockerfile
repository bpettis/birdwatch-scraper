FROM python:3.11-slim-buster

WORKDIR /birdwatch-parsing

COPY requirements.txt requirements.txt

RUN pip3 install -r requirements.txt

COPY import-tsv.py import-tsv.py
COPY .env .env

CMD [ "python3", "import-tsv.py" ]