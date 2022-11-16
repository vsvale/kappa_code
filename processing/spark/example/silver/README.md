# Build spark image
- docker login
- docker build ./repository/code/processing/spark/example/silver -t vsvale/example-silver:1.0.0; docker push vsvale/example-silver:1.0.0;
- yaml in airflow repository