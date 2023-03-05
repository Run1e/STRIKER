FROM python:3.8.16

RUN mkdir app
WORKDIR /app

COPY shared shared
COPY microservices/login-provisioner .

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python3", "-u", "provisioner.py"]