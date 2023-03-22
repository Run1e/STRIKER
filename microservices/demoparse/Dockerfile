FROM python:3.8.16

RUN mkdir app
WORKDIR /app

COPY shared shared
COPY microservices/demoparse .

RUN pip install --no-cache-dir -r requirements.txt

ENV NODE_VERSION=16.16.0
RUN curl https://nodejs.org/dist/v$NODE_VERSION/node-v$NODE_VERSION-linux-x64.tar.gz | tar -xz -C /usr/local --strip-components 1
RUN cd parse && npm install

CMD ["python3", "-u", "demoparse.py"]