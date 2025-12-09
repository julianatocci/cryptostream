#Criando scheduler
gcloud scheduler jobs create http cryptostream-gold-scheduler \
  --project="$PROJECT_ID" \
  --location="europe-west1" \
  --schedule="* * * * *" \
  --http-method=POST \
  --uri="https://run.googleapis.com/v2/projects/$PROJECT_ID/locations/europe-west1/jobs/cryptostream-gold-job:run" \
  --oauth-service-account-email="scheduler-dbt@$PROJECT_ID.iam.gserviceaccount.com" \
  --oauth-token-scope="https://www.googleapis.com/auth/cloud-platform"

# Pausando scheduler
gcloud scheduler jobs pause cryptostream-gold-scheduler \
  --location=europe-west1

# Retomando scheduler
gcloud scheduler jobs resume cryptostream-gold-scheduler \
  --location=europe-west1
  
# Deletando scheduler
gcloud scheduler jobs delete cryptostream-gold-scheduler \
  --location=europe-west1
