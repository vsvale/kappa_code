# Build spark image
- docker login
- docker build ./processing/spark/example -t vsvale/example:1.0.0; docker push vsvale/example:1.0.0;
- yaml in airflow repository