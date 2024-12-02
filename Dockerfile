# Gunakan image Python yang sesuai
FROM python:3.13.0-alpine3.20

# Tentukan working directory di dalam kontainer
WORKDIR /app

# Salin file requirements.txt ke dalam kontainer
COPY requirements.txt /app/

# Install dependensi yang dibutuhkan
RUN pip install --no-cache-dir -r requirements.txt

# Salin seluruh aplikasi Flask ke dalam kontainer
COPY . /app/

EXPOSE 5000
# Tentukan perintah untuk menjalankan aplikasi Flask
CMD ["python", "app.py"]
