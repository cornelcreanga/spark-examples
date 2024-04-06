package com.ccreanga.spark.examples.util.rdd;


import java.io.Serializable;
import java.util.List;
import java.util.Map;
public interface RecordGenerator<T> extends Serializable {
    List<T> generate(Map<String, Object> context, long itemNumber);
}
