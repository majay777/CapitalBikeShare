FROM python:3.10-slim

WORKDIR /app
RUN pip install kafka-python
RUN pip install pyspark

COPY consumer.py .

CMD ["python", "consumer.py"]
