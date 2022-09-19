FROM python:3.10

WORKDIR /opt/postgres_to_es

COPY requirements.txt requirements.txt

RUN pip3 install -r requirements.txt --no-cache-dir

COPY . .

CMD ["python3", "main.py"]