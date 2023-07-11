# Changes in Apache Kafka 1.1 topic
### Reassign partitions
1. Create file reassign-partitions.json and put data like in the template
2. Run command: <br>
 ``` /usr/hdf/current/kafka-broker/bin/kafka-reassign-partitions.sh --zookeeper ks-dmp114.kyivstar.ua:2181 --reassignment-json-file /home/oburiatov/reassign-partitions.json --execute ```

### Re-eclection leader broker in each partition
1. Create file election.json and put data like in the template
2. Run command: <br>
``` /usr/hdf/current/kafka-broker/bin/kafka-preferred-replica-election.sh --zookeeper ks-dmp114.kyivstar.ua:2181 --path-to-json-file /home/oburiatov/election.json ```
