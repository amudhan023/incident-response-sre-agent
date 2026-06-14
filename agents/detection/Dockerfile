FROM python:3.11-slim
WORKDIR /app
COPY agents/detection/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY shared/ /app/shared/
COPY agents/detection/src/ /app/src/
CMD ["python", "src/main.py"]
