FROM python:3.10-slim

WORKDIR /app
RUN pip install kafka-python
RUN pip install requests

COPY producer.py .

CMD ["python", "producer.py"]
