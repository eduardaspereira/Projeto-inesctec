# Usar uma imagem oficial do Python otimizada
FROM python:3.10-slim-bookworm

# Definir o diretório de trabalho no contentor
WORKDIR /app

# Instalar dependências do sistema necessárias para compilar algumas bibliotecas
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copiar os requirements e instala as bibliotecas Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar o script do encoder
COPY clip_encoder.py .

# Comando para correr o script
CMD ["python", "-u", "clip_encoder.py"]