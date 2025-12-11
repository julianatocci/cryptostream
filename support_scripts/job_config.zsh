 gcloud run jobs create cryptostream-trades-pipeline \                                                    [🐍 3.12.8]
  --image=europe-west1-docker.pkg.dev/wagon-bootcamp-466414/cryptostream-dbt/crypto-dbt-templateatest:latest \
  --region=europe-west1 \
  --set-env-vars=PROJECT_ID=wagon-bootcamp-466414,DBT_TARGET=prod,DBT_COMMAND=run,DBT_SELECT="staging_trades+"
