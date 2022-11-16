# Build spark image

- docker build ./repository/code/processing/spark/energy-case/landing/diesel/ -t diesel-to-landing:1.0.0
- docker tag diesel-to-landing:1.0.0 vsvale/diesel-to-landing:1.0.0
- docker login
- docker push vsvale/diesel-to-landing:1.0.0
- Put yaml in dag folder in airflow repository
- trigger dag
- kubectl logs diesel-to-landing-driver -n processing
