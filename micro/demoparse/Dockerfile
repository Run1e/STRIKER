FROM python:3.8.13-bullseye

RUN mkdir /app
WORKDIR /app

COPY micro/demoparse/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

RUN mkdir micro
COPY micro/demoparse .
COPY shared .

CMD ["python3", "-u", "start_demoparse.py"]