FROM python:3.8.13-bullseye

RUN mkdir /app
WORKDIR /app

COPY shared shared
COPY bot bot
COPY adapters adapters
COPY domain domain
COPY services services
COPY bootstrap.py .

RUN pip install --no-cache-dir -r bot/requirements.txt

CMD ["python3", "-u", "bootstrap.py"]