# Build spark image
- docker login
- docker build -f ./processing/spark/example/stream/Dockerfile ./processing/spark/example/stream -t vsvale/example-stream:1.0.0; docker push vsvale/example-stream:1.0.0;

# Landing
## During Tests
kubectl delete -f https://raw.githubusercontent.com/vsvale/kappa_k8s_config/master/repository/yamls/processing/spark/example/dimcurrency-landing.yaml -n processing;
kubectl delete -f https://raw.githubusercontent.com/vsvale/kappa_k8s_config/master/repository/yamls/processing/spark/example/dimproductsubcategory-landing.yaml -n processing;
kubectl delete -f https://raw.githubusercontent.com/vsvale/kappa_k8s_config/master/repository/yamls/processing/spark/example/dimdate-landing.yaml -n processing;

kubectl delete -f https://raw.githubusercontent.com/vsvale/kappa_k8s_config/master/repository/yamls/processing/spark/example/dimpromotion-landing.yaml -n processing;
kubectl delete -f https://raw.githubusercontent.com/vsvale/kappa_k8s_config/master/repository/yamls/processing/spark/example/dimsalesterritory-landing.yaml -n processing;
kubectl delete -f https://raw.githubusercontent.com/vsvale/kappa_k8s_config/master/repository/yamls/processing/spark/example/factinternetsalesreason-landing.yaml -n processing;

## Verify if topics exists
- dimcurrency_spark_stream_dwfiles
- dimdate_spark_stream_dwfiles
- dimproductsubcategory_spark_stream_dwfiles
- dimpromotion_spark_stream_dwfiles
- dimsalesterritory_spark_stream_dwfiles
- factinternetsalesreason_spark_stream_dwfiles

## Apply jobs

kubectl apply -f https://raw.githubusercontent.com/vsvale/kappa_k8s_config/master/repository/yamls/processing/spark/example/dimcurrency-landing.yaml -n processing;
kubectl apply -f https://raw.githubusercontent.com/vsvale/kappa_k8s_config/master/repository/yamls/processing/spark/example/dimproductsubcategory-landing.yaml -n processing;
kubectl apply -f https://raw.githubusercontent.com/vsvale/kappa_k8s_config/master/repository/yamls/processing/spark/example/dimdate-landing.yaml -n processing;
kubectl apply -f https://raw.githubusercontent.com/vsvale/kappa_k8s_config/master/repository/yamls/processing/spark/example/dimpromotion-landing.yaml -n processing;
kubectl apply -f https://raw.githubusercontent.com/vsvale/kappa_k8s_config/master/repository/yamls/processing/spark/example/dimsalesterritory-landing.yaml -n processing;
kubectl apply -f https://raw.githubusercontent.com/vsvale/kappa_k8s_config/master/repository/yamls/processing/spark/example/factinternetsalesreason-landing.yaml -n processing;

## Verify job is health and running
- kubens processing
- kubectl get pod
- kubectl logs example-dimproductsubcategory-landing-driver

# Kafka to gold
## Debug
- docker build -f ./processing/spark/example/stream/Dockerfile ./processing/spark/example/stream -t vsvale/example-stream:1.0.0; docker push vsvale/example-stream:1.0.0;

kubectl delete -f https://raw.githubusercontent.com/vsvale/kappa_k8s_config/master/repository/yamls/processing/spark/example/dimcurrency-kafka-gold.yaml


##
kubectl apply -f https://raw.githubusercontent.com/vsvale/kappa_k8s_config/master/repository/yamls/processing/spark/example/dimcurrency-kafka-gold.yaml



