FROM python:3.8.13-bullseye

RUN mkdir /app
WORKDIR /app

COPY bot/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY bot .
COPY adapters .
COPY domain .
COPY services .
COPY shared .
COPY bootstrap.py .


CMD ["python3", "-u", "bootstrap.py"]