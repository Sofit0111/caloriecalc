FROM python:3.11-slim

WORKDIR /app

# Установить зависимости
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копировать код
COPY botes.py .
COPY importer.py .

# Создать директорию для данных (для Amvera)
RUN mkdir -p /data

# Запустить бота
CMD ["python3", "botes.py"]