FROM python:3.10-slim

COPY output/*.py /app/output/
COPY requirements.txt /app/
COPY telegram2elastic.py /app/

WORKDIR /app

RUN pip install -r requirements.txt

VOLUME /sessions

ENTRYPOINT ["/app/telegram2elastic.py"]