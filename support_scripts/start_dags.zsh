
for DAG_ID in \
  cryptostream_trades_pipeline \
  cryptostream_candles_pipeline \
  cryptostream_tickers_pipeline \
  cryptostream_orderbook_pipeline \
  cryptostream_tests
do
  echo "Unpause: $DAG_ID"
  gcloud composer environments run "$COMPOSER_ENV" --location="$REGION" \
    dags unpause -- "$DAG_ID"

  echo "Trigger: $DAG_ID"
  gcloud composer environments run "$COMPOSER_ENV" --location="$REGION" \
    dags trigger -- "$DAG_ID"
done
