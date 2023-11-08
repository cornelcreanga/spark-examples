package com.ccreanga.spark.examples.partitioner;

import com.google.common.collect.ImmutableMap;

import java.io.Serializable;
import java.util.Map;

public class PartitioningInfo<K extends Serializable> {

    private final int partitionNo;
    private final Map<K, PartitionDistribution> distributionMap;

    public PartitioningInfo(int partitionNo, Map<K, PartitionDistribution> distributionMap) {
        this.partitionNo = partitionNo;
        this.distributionMap = distributionMap;
    }

    public int getPartitionNo() {
        return partitionNo;
    }

    public Map<K, PartitionDistribution> getDistributionMap() {
        return ImmutableMap.copyOf(distributionMap);
    }
}
