# Build spark image
- docker login
- docker build ./processing/spark/example/bronze -t vsvale/example-bronze:1.0.0; docker push vsvale/example-bronze:1.0.0;
- yaml in airflow repository