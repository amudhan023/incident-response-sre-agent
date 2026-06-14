FROM python:3.11-slim
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends gcc && rm -rf /var/lib/apt/lists/*
COPY knowledge/seeder/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
# Pre-download embedding model so runtime doesn't need internet
RUN python -c "from sentence_transformers import SentenceTransformer; SentenceTransformer('all-MiniLM-L6-v2'); print('Model downloaded')"
COPY shared/ /app/shared/
COPY knowledge/ /app/knowledge/
COPY knowledge/seeder/src/ /app/src/
CMD ["python", "src/main.py"]
