# Pinlater

PinLater is a Thrift service to manage scheduling and execution of asynchronous jobs.   

## Key features
- **reliable job execution**: explicit acks and automatically retries with configurable delay.
- **job scheduling**: schedule jobs to be executed at a specific time in the future
- **rate limiting**: ability to rate limit the execution of a particular queue in the system
- **language agnostic**: allows both job enqueuers and workers to be written in any thrift-supported language.
- **horizontal scalability**: both the service and storage are horizontal scalable. scaling the system up to handle more load is as easy as launching more PinLater hosts
- **multiple storage backends**: currently provide MySQL and Redis implementations for different use cases.
- **observability**: visibility into individual jobs and corresponding job queues, metrics tracking various runtime properties are exposed through [Ostrich].

## QuickStart

### Build
```sh
mvn clean compile
```

### Create and install jars
```sh
mvn clean package
mkdir ${PINLATER_INSTALL_DIR}
tar -zxvf target/pinlater-0.1-SNAPSHOT-bin.tar.gz -C ${PINLATER_INSTALL_DIR}
```

### Run Server Locally with MySQL backend
```sh
cd ${PINLATER_INSTALL_DIR}
# Make sure you have a MySQL instance running locally
java -server -cp .:./*:./lib/* -Dserver_config=pinlater.local.properties -Dbackend_config=mysql.local.json -Dlog4j.configuration=log4j.local.properties com.pinterest.pinlater.PinLaterServer
```

### Run Server Locally with Redis backend
```sh
# Make sure you have a Redis instance running locally
cd ${PINLATER_INSTALL_DIR}
java -server -cp .:./*:./lib/* -Dserver_config=pinlater.redis.local.properties -Dbackend_config=redis.local.json -Dlog4j.configuration=log4j.local.properties com.pinterest.pinlater.PinLaterServer
```

## Client Tool
A PinLater client tool for correctness and test/performance testing. 

### Create Queue
```sh
java -cp .:./*:./lib/* -Dlog4j.configuration=log4j.local.properties com.pinterest.pinlater.client.PinLaterClientTool --host localhost --port 9010 --mode create  --queue test_queue
```

### Enqueue jobs
```sh
java -cp .:./*:./lib/* -Dlog4j.configuration=log4j.local.properties  com.pinterest.pinlater.client.PinLaterClientTool --host localhost --port 9010 --mode enqueue --queue test_queue --num_queries -1 --batch_size 1 --concurrency 5
```

### Dequeue/Ack jobs
```sh
java -cp .:./*:./lib/* -Dlog4j.configuration=log4j.local.properties com.pinterest.pinlater.client.PinLaterClientTool --host localhost --port 9010 --mode dequeue --queue test_queue --num_queries -1 --batch_size 50 --concurrency 5 --dequeue_success_percent 95
```

### Look up job
```sh
java -cp .:./*:./lib/* -Dlog4j.configuration=log4j.local.properties com.pinterest.pinlater.client.PinLaterClientTool --host localhost --port 9010 --mode lookup --queue test_queue --job_descriptor test_queue:s1d1:p1:1
```
PinLater job descriptor is formatted as `[queue_name][shard_id][priority][local_id]`.

### Get queue names
```sh
java -cp .:./*:./lib/* -Dlog4j.configuration=log4j.local.properties com.pinterest.pinlater.client.PinLaterClientTool --host localhost --port 9010 --mode get_queue_names
```

### Get job count
```sh
java -cp .:./*:./lib/* -Dlog4j.configuration=log4j.local.properties com.pinterest.pinlater.client.PinLaterClientTool --host localhost --port 9010 --mode get_job_count --queue test_queue --job_state 0 --priority 1 --count_future_jobs false
```
The job state can be 0, 1, 2, or 3 (corresponds to pending, running, succeeded, and failed respectively).

## Customizing your setup

PinLater comes with sample run scripts, properties files and backend configs which can be customized to your setup. Each run script ([run_server_local_mysql.sh](src/main/scripts/run_server_local_mysql.sh), [run_server_local_redis.sh](src/main/scripts/run_server_local_redis.sh)) contains environment variables which you may modify according to your setup. It’s highly likely that you would need to modify the following variables:

- PINLATER_HOME_DIR: Directory containing extracted jars.
- SERVER_CONFIG: Your cluster properties file
- BACKEND_CONFIG: Your storage backend config file
- RUN_DIR: Directory for writing PID files

Before running the PinLater service, you’ll need to modify the backend JSON configs to point to the desired backend storage. Note that the MySQL and Redis implementation uses config files with different JSON schema. Check out [mysql.local.json](src/main/config/mysql.local.json) and [redis.local.json](src/main/config/redis.local.json) for examples. You will also need to modify the log4j properties file to point to the desired directories for the application logs for the controller, server and thrift server respectively.

There are several important properties that you might also want to modify:

```
# Timeout for preventing jobs from running forever. Timeout jobs will be 
# retried or marked as failed depending on the number of attempts remained.
BACKEND_MONITOR_JOB_CLAIMED_TIMEOUT_SECONDS=600
# TTL of failed jobs before they are cleaned up
BACKEND_MONITOR_JOB_FAILED_GC_TTL_HOURS=168
# TTL of succeeded jobs before they are cleaned up
BACKEND_MONITOR_JOB_SUCCEEDED_GC_TTL_HOURS=24
```

To start and stop the service:
```sh
# Start the PinLater service with MySQL
scripts/run_server_local_mysql.sh start

# Stop the PinLater service with MySQL
scripts/run_server_local_mysql.sh stop
```

```sh
# Start the PinLater service with Redis
scripts/run_server_local_redis.sh start

# Stop the PinLater service with Redis
scripts/run_server_local_redis.sh stop
```

## Usage

### Build a worker
A PinLater worker is responsible for continuously dequeuing jobs, execute them and then reply to the PinLater server with a positive or negative ACK, depending on whether the execution succeeded or failed. PinLater allows the worker to optionally specify a retry delay for a job failure. It can be used to implement arbitrary retry policies per job, e.g. constant delay retry, exponential backoff, or a combination thereof.

We provide an example worker as a reference implementation which handles all above. It executes an example PinLater job defined in [PinLaterExampleJob.java](src/main/java/com/pinterest/pinlater/example/PinLaterExampleJob.java). For more detail, check out [PinLaterExampleWorker.java](src/main/java/com/pinterest/pinlater/example/PinLaterExampleWorker.java). To run the example worker:

```sh
cd ${PINLATER_INSTALL_DIR}
# Use the client tool to create test_queue and enqueue jobs
java -server -cp .:./*:./lib/* -Dserverset_path=discovery.pinlater.local -Dlog4j.configuration=log4j.local.properties com.pinterest.pinlater.example.PinLaterExampleWorker
```

We also provide a Java [client](src/main/java/com/pinterest/pinlater/client/PinLaterClient.java) used by the example worker and client tool. Client can also be implemented in any Thrift-supported language with the [thrift interface](src/main/thrift/pinlater.thrift). A job is language agnostic and its job body is just a sequence of bytes from the PinLater service’s perspective. Therefore jobs can even be enqueued and dequeued by clients in different languages.

### Dequeue rate limiting
PinLater provides per-queue rate limiting that allows an operator to limit the dequeue rate on any queue in the system, or even stop dequeues completely, which can help alleviate load quickly on a struggling backend system, or prevent a slow job from affecting other jobs.

Rate limiting is configured via a JSON config file, which PinLater automatically reloads when any change happen. Here is an example queue config file:

```json
{
    "queues": [
        {
            "name": "pinlater_test_queue",
            "queueConfig": {
                "maxJobsPerSecond": 100
            }
        },
        {
            "name": "pinlater_test_slow_queue",
            "queueConfig": {
                "maxJobsPerSecond": 0.1
            }
        },
        {
            "name": "pinlater_test_paused_queue",
            "queueConfig": {
                "maxJobsPerSecond": 0
            }
        }
    ]
}
```

Rate limiting also depends on [ServerSet] to figure out how many PinLater servers are active and compute the per-server rate limit. We use a file based ServerSet implementation as an example, which uses a local file that contains a `[HOSTNAME]:[PORT]` pair on each line. ServerSet can be configured by setting `SERVER_SET_ENABLED` and `SERVER_SET_PATH`. If serverset is disable, rate limits will be applied per server.

### Storage backends

PinLater has two implementations: one built on top of Redis and one built on top of MySQL. In general services should default to use the MySQL backend as long as the QPS is in the lower to mid range (no more than 1000 QPS per shard). If the QPS is expected to be higher than this, then the Redis implementation should be used. The main advantage using MySQL over Redis is the amount of available space to store jobs is far greater; Redis backed services run the risk of incurring data loss if pending job back ups are not tended to in a few hours. Both MySQL and Redis implementations use a JSON file for backend configuration. (Note: the MySQL backend supports automatic reload of backend configuration, which can help implement features like auto failover. It’s not yet implemented in the Redis backend).

PinLater also supports a dequeue-only mode where a shard only receives dequeue requests but no new jobs will be enqueued. It can help dealing with struggling backend system, or drain a shard before downsizing the cluster. Check out [mysql.local.json](src/main/config/mysql.local.json) and [redis.local.json](src/main/config/redis.local.json) for examples.

## Monitoring
PinLater provides a set of APIs through the thrift interface for monitoring queue status and managing jobs, including getQueueNames, getJobCount, lookupJob, scanJobs and etc. For more details, please check out the [thrift interface](src/main/thrift/pinlater.thrift)

You can also retrieve detailed metrics by running **curl localhost:9999/stats.txt**
on the server. These metrics are exported using Twitter's [Ostrich]
library and are easy to parse. The port can be modified by setting the
`ostrich_metrics_port` property.

## Design
If you are interested in the detailed design, check out our [blog post about PinLater](https://engineering.pinterest.com/blog/pinlater-asynchronous-job-execution-system)

[Ostrich]: https://github.com/twitter/ostrich
[ServerSet]: https://twitter.github.io/commons/apidocs/com/twitter/common/zookeeper/ServerSet.html

