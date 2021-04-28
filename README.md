# kafka-twitter-analyzer
Kafka Consumer application that builds analytical reports for Twitter tweets.

## Prepare

```bash
npm i
mkdir -p data && cd data && kaggle datasets download -d kazanova/sentiment140
```

Then, add GCP credentials JSON file and save path to it in environment variable `GOOGLE_APPLICATION_CREDENTIALS`.

Also, save list of Kafka brokers (separated by comma) in another environment variable `KAFKA_BROKERS`.

## Run

Finally, we ready to start:

```bash
npm start
```

