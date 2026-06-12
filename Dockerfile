# Use Python 3.11 slim image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

# Install system dependencies including Node.js for Photon attachment SDK
RUN apt-get update && apt-get install -y \
    ca-certificates \
    gcc \
    python3-dev \
    supervisor \
    curl \
    && curl -fsSL https://deb.nodesource.com/setup_20.x | bash - \
    && apt-get install -y nodejs \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies (use legacy resolver for complex dependency trees)
RUN pip install --upgrade pip && \
    pip install -r requirements.txt --use-deprecated=legacy-resolver

# Copy scripts directory and install Node.js dependencies for attachment sending
COPY support/scripts/ ./scripts/
RUN cd scripts && npm install --production && cd ..

# Copy application code
COPY app/ ./app/

# Copy supervisor config (runs uvicorn + background workers)
COPY infrastructure/supervisor/supervisord.conf /etc/supervisor/conf.d/supervisord.conf

# Create non-root user
RUN useradd -m -u 1000 appuser && \
    chown -R appuser:appuser /app

# Switch to non-root user
USER appuser

# Expose port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import requests; requests.get('http://localhost:8000/health')"

# Run FastAPI with uvicorn
CMD ["supervisord", "-c", "/etc/supervisor/conf.d/supervisord.conf"]
