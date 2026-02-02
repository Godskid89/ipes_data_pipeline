# Use official Playwright image which includes Python and Browsers
# This saves us from installing Chromium manually
FROM mcr.microsoft.com/playwright/python:v1.41.0-jammy

WORKDIR /app

# Install dependencies first (caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY . .

# Create output directories
RUN mkdir -p data/raw data/structured data/enriched documents

# Define entrypoint
ENTRYPOINT ["python3", "run_pipeline.py"]
