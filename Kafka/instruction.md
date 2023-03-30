1. Create file reassign-partitions.json
2. /usr/hdf/current/kafka-broker/bin/kafka-reassign-partitions.sh --zookeeper ks-dmp73.kyivstar.ua:2181 --reassignment-json-file /home/oburiatov/reassign-partitions.json --execute


3. /usr/hdf/current/kafka-broker/bin/kafka-preferred-replica-election.sh --zookeeper ks-dmp114.kyivstar.ua:2181 --path-to-json-file /home/oburiatov/election.json
