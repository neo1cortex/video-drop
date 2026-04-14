FROM python:3.12-slim

RUN apt-get update && apt-get install -y \
    ffmpeg \
    intel-media-va-driver \
    libva-drm2 \
    libva-x11-2 \
    vainfo \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /code

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8093

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8093"]