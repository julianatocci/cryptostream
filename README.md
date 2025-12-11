# CryptoStream – Pipeline Realtime de Mercado Cripto (GCP + dbt + Looker Studio)

## Visão geral

Este projeto implementa um pipeline de dados em **streaming** utilizando a API WebSocket
da Binance Spot, a stack de dados da **Google Cloud Platform** e o **dbt** para modelagem
analítica. Os dados resultantes alimentam um dashboard em **Looker Studio** focado em
monitorar o mercado cripto em tempo quase real.

Arquitetura resumida:

- **Ingestão:** WebSocket Binance → Publisher Python → Pub/Sub
- **Bronze:** mensagens brutas em envelopes JSON no BigQuery
- **Staging:** views tipadas por tipo de evento (trades, kline, depth, ticker)
- **Silver:** tabelas incrementais deduplicadas e normalizadas
- **Gold:** tabelas otimizadas para consumo de dashboard (OHLCV, candles oficiais,
  tickers agregados, métricas de liquidez)

---

## Camadas de dados

### Bronze

- Dataset: `crypto_data_raw`
- Tabelas brutas por tipo de stream:
  - `raw_trades`
  - `raw_candles`
  - `raw_orderbook`
  - `raw_tickers`

Cada linha é um envelope com metadados (`stream`, `ingest_ts`, `payload`) e o JSON bruto
do evento WebSocket da Binance.

---

### Staging

Views de staging (dbt) que:

- apontam para as tabelas bronze;
- extraem os campos relevantes do JSON;
- convertem tipos (timestamp, numeric, bool);
- mantêm o schema próximo do payload original.

Principais views:

- `staging_trades` – eventos `<symbol>@trade`
- `staging_candles` – eventos `<symbol>@kline_<interval>`
- `staging_orderbook` – eventos `<symbol>@depth`
- `staging_tickers` – eventos `<symbol>@ticker`

---

### Silver

Tabelas silver **incrementais** que:

- deduplicam reentregas do pipeline (`row_number()` + `ingest_ts`);
- aplicam filtros mínimos de integridade (ex.: `price > 0`, `qty > 0`);
- adicionam colunas de apoio:
  - `event_date`
  - `minute_bucket`
  - `hour_bucket`

Principais tabelas:

- `silver_trades`
- `silver_candles`
- `silver_orderbook`
- `silver_tickers`

Essas tabelas são **particionadas por tempo** e **clusterizadas por symbol**, reduzindo
custo de leitura e preparando o terreno para a camada gold.

---

## Camada Gold

A camada gold é onde as tabelas são desenhadas explicitamente para o consumo do
dashboard, com foco em **baixa latência** e **baixo custo**.

### 1. Gold de trades – `gold_trades_ohlcv_*`

Família de tabelas que reconstrói candles OHLCV a partir dos trades:

- `gold_trades_ohlcv_1m`
- `gold_trades_ohlcv_5m`
- `gold_trades_ohlcv_15m`
- `gold_trades_ohlcv_1h`
- `gold_trades_ohlcv_1d`

**Papel:**

- a tabela de **1 minuto** é construída diretamente de `silver_trades`;
- as tabelas de grains maiores (5m, 15m, 1h, 1d) são construídas **a partir da 1m**;
- evita reler a silver e garante consultas baratas para o dashboard.

Uso principal:

- gráficos de **volume por tempo**;
- análises de OHLCV reconstruídas via trades (por exemplo, VWAP e volatilidade).

---

### 2. Gold de candles oficiais – `gold_candles_closed_*`

Família baseada no stream de **kline** da Binance:

- `gold_candles_closed_1m`
- `gold_candles_closed_5m`
- `gold_candles_closed_15m`
- `gold_candles_closed_1h`
- `gold_candles_closed_1d`

**Papel:**

- `gold_candles_closed_1m` lê diretamente de `silver_candles` e mantém apenas candles
  com `is_closed = true`;
- grains maiores são agregações da tabela de 1m;
- representa a visão **oficial** da exchange para cada intervalo.

Uso principal:

- **gráficos de candle** no dashboard (1m, 5m, 15m, 1h, 1d);
- comparações entre candles “oficiais” vs candles reconstruídas via trades.

---

### 3. Gold de tickers – `gold_tickers_1m` e `gold_tickers_latest`

#### `gold_tickers_1m`

- Timeseries leve com snapshots de ticker agregados por minuto.
- Principais campos: `last_price`, `best_bid_price`, `best_ask_price`, `spread`,
  `mid_price`, `price_change_percent`, `base_volume_24h`, `quote_volume_24h`.

Uso:

- gráficos de **preço por minuto**;
- visão suave de tendência e variação de 24h ao longo do tempo.

#### `gold_tickers_latest`

- Mantém **uma linha por símbolo**, com o snapshot mais recente de ticker.
- Principais campos:
  - preço atual (`last_price`);
  - spread e mid price;
  - variação 24h (absoluta e percentual);
  - volumes e número de trades em 24h.

Uso:

- base dos **cards de overview** do dashboard;
- permite refresh frequente com uma tabela muito pequena, sem custo alto.

---

### 4. Gold de orderbook – `gold_orderbook_liquidity_1m`

- Explode as listas de `bids` e `asks` da `silver_orderbook`;
- agrega por símbolo e minuto;
- calcula:
  - `best_bid_price`
  - `best_ask_price`
  - `spread`
  - `mid_price`
  - `total_bid_qty`
  - `total_ask_qty`

Uso:

- visualizações de **tightness** (spread) e **profundidade de liquidez**;
- gráficos de desequilíbrio (mais compra vs mais venda).

---

## Dashboard no Looker Studio

O dashboard é organizado em seções temáticas, cada uma baseada em tabelas gold
otimizadas:

1. **Market Overview**
   - Fonte principal: `gold_tickers_latest`
   - KPIs de preço, variação 24h, spread e volume.
   - Heatmap de variação por símbolo.

2. **Price Action & Candles**
   - Fontes:
     - `gold_tickers_1m` (linha de preço por minuto)
     - `gold_candles_closed_*` (candles oficiais 1m/5m/15m/1h/1d)
   - Gráficos de candle interativos com seleção de timeframe.

3. **Volume & Atividade**
   - Fontes:
     - `gold_trades_ohlcv_1m/5m/15m/1h/1d`
   - Gráficos de volume por tempo e ranking de ativos por volume.

4. **Liquidez & Spread**
   - Fonte: `gold_orderbook_liquidity_1m`
   - Séries temporais de spread, profundidade bid/ask e indicadores de desequilíbrio.

5. **Asset Deep Dive**
   - Combina todas as fontes para um símbolo escolhido:
     - candles, volume, spread, VWAP, volatilidade;
     - permite uma análise detalhada da microestrutura do ativo.

---

## Decisões de projeto

- **Camada gold separada por família (trades, candles, tickers, orderbook)**
  Facilita o reuso, documentação e evolução independente de cada tipo de dado.

- **Uso intensivo de tabelas derivadas de “1m”**
  Todos os grains maiores (5m, 15m, 1h, 1d) são calculados a partir da tabela de 1 minuto,
  reduzindo custo de leitura em silver e simplificando o raciocínio.

- **Snapshots leves para o dashboard (`gold_tickers_latest`)**
  Em vez de o dashboard consultar diretamente a silver ou recalcular a “última linha”,
  uma tabela minúscula mantém o snapshot de mercado, garantindo baixo custo e
  tempo de resposta baixo.

- **Particionamento por tempo e clusterização por símbolo**
  Todas as tabelas incrementais usam `event_ts`/`bucket_ts` como campo de partição
  e `symbol` como cluster. Isso reduz custo de scan no BigQuery e mantém o projeto
  escalável para múltiplos pares de negociação.

---
