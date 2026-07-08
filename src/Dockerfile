# Usar uma imagem oficial do Python otimizada
FROM python:3.10-slim-bookworm

# Definir o diretório de trabalho no contentor
WORKDIR /app

# Instalar dependências do sistema necessárias
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    git \
    && rm -rf /var/lib/apt/lists/*

# Copiar os requirements e instalar as bibliotecas Python (Caminho corrigido para a raiz)
COPY src/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar o script do encoder (Caminho corrigido para a raiz)
COPY src/clip_encoder.py .

# Comando para correr o script
CMD ["python", "-u", "clip_encoder.py"]