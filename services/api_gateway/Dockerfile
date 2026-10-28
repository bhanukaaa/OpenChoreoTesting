FROM python:3.9-slim
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY services/api_gateway/ .

RUN groupadd -g 10001 appuser && useradd -u 10001 -g appuser -s /bin/sh -m appuser
USER 10001

ENV PYTHONUNBUFFERED=1
EXPOSE 8080

CMD ["python", "app.py"]
