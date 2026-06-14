FROM python:3.11-slim
WORKDIR /app
COPY agents/communication/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY shared/ /app/shared/
COPY agents/communication/src/ /app/src/
COPY agents/communication/templates/ /app/templates/
CMD ["python", "src/main.py"]
