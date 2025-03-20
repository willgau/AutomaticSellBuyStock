FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
RUN pip install alpaca_trade_api
RUN pip install aioredis

COPY solution/*.py .
