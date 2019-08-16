### Submit

```
./spark-submit --class Main --master spark://localhost:7077 app.jar
```

```
docker run --rm --network bigdata_deploy_default -e ENABLE_INIT_DAEMON=false -e SPARK_APPLICATION_ARGS="broker:29092 carlocate sparkGroup" --link spark-master:spark-master --link broker:broker --link zookeeper:zookeeper spark-digest
```


Create keyspace and table in Cassandra:
```
CREATE KEYSPACE spar WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};      
CREATE TABLE test.carlocation ( location text, PRIMARY KEY(LOCATION));
```



Create Docker Image:

```
docker build -t spark-digest .
```

Lanuch:
```
docker run --rm --network deployment_default -e ENABLE_INIT_DAEMON=false -e SPARK_APPLICATION_ARGS="broker:29092 cars sparkGroup2 cassandra-1"  spark-digest
```