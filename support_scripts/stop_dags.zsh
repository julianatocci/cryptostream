for DAG_ID in \
  cryptostream_trades_pipeline \
  cryptostream_candles_pipeline \
  cryptostream_tickers_pipeline \
  cryptostream_orderbook_pipeline \
  cryptostream_tests
do
  echo "Pause: $DAG_ID"
  gcloud composer environments run "$COMPOSER_ENV" --location="$REGION" \
    dags pause -- "$DAG_ID"
done
