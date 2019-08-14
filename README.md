### Submit

```
./spark-submit --class Main --master spark://localhost:7077 app.jar
```

```
docker run --rm --network bigdata_deploy_default -e ENABLE_INIT_DAEMON=false -e SPARK_APPLICATION_ARGS="broker:29092 carlocate sparkGroup" --link spark-master:spark-master --link broker:broker --link zookeeper:zookeeper spark-digest
```