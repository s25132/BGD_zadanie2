FROM python:3.11-slim

# Set working directory
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY *.py ./

# Default command: run the pipeline
CMD ["python", "pipeline.py"]