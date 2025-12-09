FROM python:3.12-slim

# Variáveis globais
ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    DBT_PROFILES_DIR=/app/cryptostream \
    DBT_PROJECT_DIR=/app/cryptostream

# Ainda como root
WORKDIR /app

# Dependências de sistema mínimas
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
 && rm -rf /var/lib/apt/lists/*

# Criar usuário/grupo non-root
RUN groupadd -r dbt && useradd -r -g dbt dbt

# Primeiro copia só o que é necessário para resolver dependências
# IMPORTANTE: incluir README.md por causa do pyproject.toml
COPY pyproject.toml poetry.lock* README.md /app/

# Instala poetry e dependências do projeto (como root)
RUN pip install --upgrade pip \
 && pip install poetry \
 && poetry config virtualenvs.create false \
 # não instalar o package em si, só as deps
 && poetry install --no-interaction --no-ansi --no-root

# Agora copia o restante do projeto (incluindo a pasta cryptostream/)
COPY . /app

# Garantir que o usuário dbt tenha acesso aos arquivos
RUN chown -R dbt:dbt /app

# A partir daqui, roda como non-root
USER dbt

# dbt vai rodar a partir da pasta do projeto
WORKDIR /app/cryptostream

# Valor default; no Cloud Run Job a gente sobrescreve com DBT_TARGET=prod, se quiser
ENV DBT_TARGET=dev

# Você pode sobrescrever DBT_SELECT via env var no Cloud Run Job
CMD ["bash", "-c", "dbt run --select ${DBT_SELECT:-+gold}"]
