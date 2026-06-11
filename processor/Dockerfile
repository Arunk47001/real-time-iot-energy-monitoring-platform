FROM python:3.12-slim

WORKDIR /opt/processor

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

RUN useradd --create-home appuser
USER appuser

CMD ["python", "-m", "app.main"]
