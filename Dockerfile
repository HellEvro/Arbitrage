FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY pyproject.toml README.md ./
COPY config ./config
COPY arbitrage_bot ./arbitrage_bot
COPY main.py ./main.py

RUN apt-get update && apt-get install -y --no-install-recommends build-essential && rm -rf /var/lib/apt/lists/* \
    && pip install --upgrade pip \
    && pip install .

EXPOSE 5152

CMD ["python", "main.py"]

