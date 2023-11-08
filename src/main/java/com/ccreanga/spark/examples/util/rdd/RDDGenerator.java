package com.ccreanga.spark.examples.util.rdd;

import org.apache.spark.Partition;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext;
import org.apache.spark.rdd.RDD;
import scala.collection.Iterator;
import scala.collection.JavaConverters;
import scala.collection.mutable.ArrayBuffer;
import scala.reflect.ClassManifestFactory$;

import java.util.Map;


public class RDDGenerator<T> extends RDD<T> {

    private final int numSlices;
    private final long numValues;
    private final Map<String, Object> context;
    private final RecordGenerator<T> recordGenerator;

    private final int valuesPerSlice;
    private final int slicesWithExtraItem;

    public static <T> RDDGenerator<T> of(SparkContext sc, int numSlices, long numValues, Map<String, Object> context, RecordGenerator<T> recordGenerator, Class<T> type) {
        if (numSlices <= 0 || numSlices >= 10000)
            throw new RuntimeException("numSlices should be between 0 and 10000");
        if (numValues <= 0)
            throw new RuntimeException("numValues should be positive");

        return new RDDGenerator<>(sc, numSlices, numValues, context, recordGenerator, type);
    }

    RDDGenerator(SparkContext sc, int numSlices, long numValues, Map<String, Object> context, RecordGenerator<T> recordGenerator, Class<T> type) {
        super(sc, new ArrayBuffer<>(), ClassManifestFactory$.MODULE$.fromClass(type));
        this.numSlices = numSlices;
        this.numValues = numValues;
        this.context = context;
        this.recordGenerator = recordGenerator;
        valuesPerSlice = (int)numValues / numSlices;
        slicesWithExtraItem = (int)numValues % numSlices;
    }


    @Override
    public Iterator<T> compute(Partition split, TaskContext context) {
        return JavaConverters.asScalaIterator(((RDDGeneratorPartition<T>) split).values().iterator());
    }

    @Override
    public Partition[] getPartitions() {
        Partition[] partitions = new Partition[numSlices];
        for (int i = 0; i < slicesWithExtraItem; i++) {
            partitions[i] = new RDDGeneratorPartition<>(i, valuesPerSlice + 1, context, recordGenerator);
        }
        for (int i = slicesWithExtraItem; i < numSlices; i++) {
            partitions[i] = new RDDGeneratorPartition<>(i, valuesPerSlice, context, recordGenerator);
        }
        return partitions;
    }

    @Override
    public long count() {
        return numValues;
    }


}
