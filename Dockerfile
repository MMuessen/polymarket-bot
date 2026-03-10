FROM python:3.13-slim

# Step 1: Install system essentials (Cached unless this line changes)
RUN apt-get update && apt-get install -y build-essential && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Step 2: Copy ONLY requirements first to leverage Docker cache
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Step 3: Copy the rest of the app (This is the only part that will rebuild on code changes)
COPY . .

CMD ["uvicorn", "app.main_v2:app", "--host", "0.0.0.0", "--port", "8000"]
