FROM python:3.8.13-bullseye

RUN mkdir /app
WORKDIR /app

COPY micro/matchinfo/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

RUN mkdir micro
COPY micro/matchinfo .
COPY shared .

CMD ["python3", "-u", "start_matchinfo.py"]