# Start from slim Python image
FROM python:3.11-slim

# Install system dependencies needed for building extensions and ODBC
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       curl \
       gnupg \
       apt-transport-https \
       ca-certificates \
       build-essential \
       unixodbc-dev \
       gcc \
       g++ \
       iputils-ping \
       net-tools \
    && rm -rf /var/lib/apt/lists/*

# (Optional) Install Microsoft ODBC Driver for SQL Server (linux) so pyodbc can connect to MSSQL
# Use gpg --dearmor instead of the deprecated `apt-key` to avoid "command not found" (exit code 127)
RUN set -eux; \
    curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor > /etc/apt/trusted.gpg.d/microsoft.gpg; \
    curl -fsSL https://packages.microsoft.com/config/debian/11/prod.list -o /etc/apt/sources.list.d/mssql-release.list; \
    apt-get update; \
    ACCEPT_EULA=Y apt-get install -y msodbcsql18; \
    rm -rf /var/lib/apt/lists/*

# Create app directory
WORKDIR /app

# Copy project files
COPY . /app

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Run the listener
CMD ["python", "zkteco_listener.py"]
