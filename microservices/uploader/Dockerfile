FROM python:3.11.3-slim

RUN mkdir app
WORKDIR /app

COPY shared shared
COPY messages messages
COPY microservices/uploader .

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python3", "-u", "uploader.py"]