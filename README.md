# wasp-cloud
wasp cloud assignment

This github evaluates to use Spark on the GoogleCloud  for  improving  the  performance  of  matrix  computations comparing with running computations on local machine. 

## Compile:

```
mvn clean install
mvn package
```

## Run on dataproc:
```    
gcloud dataproc jobs submit spark \
    --cluster $CLUSTER_NAME$ \
    --properties spark.streaming.receiver.writeAheadLog.enabled=true,spark.executor.memory=12g,\
    spark.driver.memory=12g,spark.num.executors=20 \
    --jar $jar in cloud storage, our jar is gs://dataproc-b990f27d-3160-4cb9-9d86-1c88d27cbf84-us/cloud.jar$ 
```
