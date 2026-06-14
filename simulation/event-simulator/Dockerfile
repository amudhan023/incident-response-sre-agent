FROM python:3.11-slim
WORKDIR /app
COPY simulation/event-simulator/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY shared/ /app/shared/
COPY simulation/event-simulator/src/ /app/src/
EXPOSE 8100
CMD ["python", "src/main.py"]
