FROM python-3.11-bullseye

COPY output/*.py /app/output/
COPY requirements.txt telegram2elastic.py /app/

WORKDIR /app

RUN pip install -r requirements.txt

VOLUME /sessions

ENTRYPOINT ["/app/telegram2elastic.py"]