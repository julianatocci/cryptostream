FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    DBT_PROFILES_DIR=/app \
    DBT_PROJECT_DIR=/app

WORKDIR /app

# 1) deps de sistema como root
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
  && rm -rf /var/lib/apt/lists/*

# 2) copia apenas arquivos de dependência (melhora cache)
COPY pyproject.toml poetry.lock* /app/

# 3) instala poetry + deps python como root
RUN pip install --upgrade pip \
 && pip install poetry \
 && poetry config virtualenvs.create false \
 && poetry install --no-interaction --no-ansi

# 4) copia o resto do projeto
COPY . /app

# 5) cria user não-root e dá permissão no /app
RUN useradd -m -u 10001 dbtuser \
 && chown -R dbtuser:dbtuser /app

# 6) troca para usuário não-root
USER dbtuser

# 7) comando default
CMD ["bash", "-c", "dbt run --select ${DBT_SELECT:-+gold}"]
