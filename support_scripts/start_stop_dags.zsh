for DAG_ID in cryptostream_pipelines cryptostream_tests; do
  gcloud composer environments run "$COMPOSER_ENV" \
    --location="$REGION" \
    dags trigger -- "$DAG_ID"
done


for DAG_ID in cryptostream_pipelines cryptostream_tests; do
  gcloud composer environments run "$COMPOSER_ENV" \
    --location="$REGION" \
    dags pause -- "$DAG_ID"
done
