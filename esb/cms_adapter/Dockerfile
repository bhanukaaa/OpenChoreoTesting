FROM python:3.9-slim
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY utility/ ./utility/
COPY esb/cms_adapter/ .

RUN groupadd -g 10001 appuser && useradd -u 10001 -g appuser -s /bin/sh -m appuser
USER 10001

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1
EXPOSE 8083

CMD ["python", "app.py"]
