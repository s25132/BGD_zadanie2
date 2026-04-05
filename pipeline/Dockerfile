FROM python:3.11-slim

# Set working directory
WORKDIR /app

RUN apt-get update && apt-get install -y \
    default-jdk \
    wget \
    curl \
    procps \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH=$JAVA_HOME/bin:$PATH
ENV PYTHONUNBUFFERED=1

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY *.py ./

# Default command: run the pipeline
CMD ["python", "pipeline.py"]