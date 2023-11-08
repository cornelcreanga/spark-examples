package com.ccreanga.spark.examples.partitioner;

import com.ccreanga.spark.examples.util.FastRandom;
import org.apache.commons.math3.distribution.EnumeratedDistribution;
import org.apache.commons.math3.util.Pair;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.List;

public class PartitionDistribution implements Serializable {

    private static final FastRandom random = new FastRandom();

    private Integer singlePartition;
    private EnumeratedDistribution<Integer> enumeratedDistribution;

    private void writeObject(ObjectOutputStream out) throws IOException {
        if (singlePartition != null) {
            out.writeInt(singlePartition);
        } else {
            out.writeInt(-1);
            out.writeObject(enumeratedDistribution);
        }
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        int p = in.readInt();
        if (p > -1) {
            singlePartition = p;
        } else {
            enumeratedDistribution = (EnumeratedDistribution) in.readObject();
        }

    }

    public PartitionDistribution(Integer singlePartition) {
        this.singlePartition = singlePartition;
    }

    public PartitionDistribution(List<Pair<Integer, Double>> probabilities) {
        this.enumeratedDistribution = new EnumeratedDistribution<>(random, probabilities);
    }

    public int getPartition() {
        if (singlePartition != null) {
            return singlePartition;
        } else {
            return enumeratedDistribution.sample();
        }
    }

    public EnumeratedDistribution<Integer> getEnumeratedDistribution() {
        return enumeratedDistribution;
    }
}
