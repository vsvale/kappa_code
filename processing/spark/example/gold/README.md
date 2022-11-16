# Build spark image
- docker login
- docker build ./repository/code/processing/spark/example/gold -t vsvale/example-gold:1.0.0; docker push vsvale/example-gold:1.0.0;
- yaml in airflow repository