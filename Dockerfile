FROM python:3.11-slim
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
COPY handlers ./handlers

ENV PORT=8080
CMD ["gunicorn", "-b", ":8080", "app.main:app"]
