package com.ccreanga.spark.examples.partitioner;

import java.io.Serializable;

public interface CostFunction<V> extends Serializable {
    long computeCost(V data);
}
