package com.ccreanga.spark.examples.partitioner;

import java.io.Serializable;

public class PartitionInfo implements Serializable {

    private final int partition;
    private final double probability;

    public PartitionInfo(int partition, double probability) {
        this.partition = partition;
        this.probability = probability;
    }

    public int getPartition() {
        return partition;
    }

    public double getProbability() {
        return probability;
    }

}
