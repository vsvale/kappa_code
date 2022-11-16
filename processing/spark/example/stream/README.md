# Build spark image
- docker login
- docker build ./processing/spark/example/stream -t vsvale/example-stream:1.0.0; docker push vsvale/example-stream:1.0.0;
- kubectl apply -f https://raw.githubusercontent.com/vsvale/kappa_code/main/processing/spark/example/stream/dimcurrency-landing.yaml -n processing
- kubectl logs example-dimcurrency-landing-driver