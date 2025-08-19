FROM python:3.9-slim

WORKDIR /app

# Устанавливаем только необходимые утилиты
RUN apt-get update && apt-get install -y \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY generate_data.py .

CMD ["python", "generate_data.py"]