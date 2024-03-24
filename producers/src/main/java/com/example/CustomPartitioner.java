package com.example;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.internals.StickyPartitionCache;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;
import java.util.Map;

public class CustomPartitioner implements Partitioner {

    private static final Logger logger = LoggerFactory.getLogger(CustomPartitioner.class);
    private final StickyPartitionCache stickyPartitionCache = new StickyPartitionCache();

    private String specialKeyName;

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        if (key == null) {
            return stickyPartitionCache.partition(topic, cluster);
        }
        List<PartitionInfo> partitionInfos = cluster.partitionsForTopic(topic);
        int numPartitions = partitionInfos.size();
        int numSpecialPartitions = numPartitions / 2;
        int partitionIndex;
        if (key.equals(specialKeyName)) {
            partitionIndex = Utils.toPositive(Utils.murmur2(valueBytes)) % numSpecialPartitions;
        } else {
            partitionIndex = Utils.toPositive(Utils.murmur2(keyBytes)) % (numPartitions - numSpecialPartitions) + 2;
        }

        logger.info("key:{} is sent to partition:{}", key, partitionIndex);
        return partitionIndex;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {
        specialKeyName = configs.get("custom.specialKey").toString();
    }
}
