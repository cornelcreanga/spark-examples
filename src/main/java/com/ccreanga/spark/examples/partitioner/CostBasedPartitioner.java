package com.ccreanga.spark.examples.partitioner;

import org.apache.commons.math3.util.Pair;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

public class CostBasedPartitioner<K extends Serializable, V> extends Partitioner implements Serializable {

    private int partitions;
    private int reservedPartitions;
    private Map<K, PartitionDistribution> distribution;

    private void writeObject(ObjectOutputStream out) throws IOException {
        out.writeInt(partitions);
        out.writeInt(reservedPartitions);
        out.writeObject(distribution);

    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        partitions = in.readInt();
        reservedPartitions = in.readInt();
        distribution = (Map<K, PartitionDistribution>) in.readObject();
    }

    public CostBasedPartitioner(Map<K, Long> freqs, int minimumPartitions, long partitionCapacity) {
        PartitioningInfo<K> partitioningInfo = computePartitionDistribution(freqs, partitionCapacity, minimumPartitions);
        this.distribution = partitioningInfo.getDistributionMap();
        this.partitions = partitioningInfo.getPartitionNo();
        this.reservedPartitions = 0;
    }

    public CostBasedPartitioner(Map<K, PartitionDistribution> precomputedDistribution, int partitions, int reservedPartitions) {
        this.distribution = precomputedDistribution;
        this.partitions = partitions;
        this.reservedPartitions = reservedPartitions;
    }

    public CostBasedPartitioner(JavaPairRDD<K, V> pairRDD, int minimumPartitions, long partitionCapacity, CostFunction<V> itemCostFunction, CostFunction<K> keyFixedCostFunction) {
        Map<K, Long> freqs = new HashMap<>(pairRDD.mapToPair(t -> new Tuple2<>(t._1, itemCostFunction.computeCost(t._2))).reduceByKeyLocally(Long::sum));
        Set<K> keys = freqs.keySet();
        for (K key : keys) {
            Long value = freqs.get(key);
            freqs.put(key, value + keyFixedCostFunction.computeCost(key));
        }
        PartitioningInfo<K> partitioningInfo = computePartitionDistribution(freqs, partitionCapacity, minimumPartitions);
        this.distribution = partitioningInfo.getDistributionMap();
        this.partitions = partitioningInfo.getPartitionNo();
        this.reservedPartitions = 0;
    }

    public PartitioningInfo<K> computePartitionDistribution(Map<K, Long> keysToCost, long partitionCost, int minPartitions) {

        long no = 0;
        List<K> keys = new ArrayList<>(keysToCost.keySet());
        List<Long> keyCosts = new ArrayList<>(keysToCost.values());
        double[] keysWeights = new double[keyCosts.size()];
        for (long cost : keyCosts) {
            if (no > (Long.MAX_VALUE - cost)){
                throw new RuntimeException("total cost exceeds Long.MAX_VALUE. Use smaller numbers");
            }
            no += cost;
        }
        //compute how many partitions do we need
        long partitions = Math.max(no / partitionCost, minPartitions);
        if (partitions > 100000){
            throw new RuntimeException("too many partitions:"+partitions);
        }
        int partitionCostRecomputed = (int) (no / partitions);
        //compute weights per item. if weight is more than 1 the items will be located in more than one partition
        for (int i = 0; i < keyCosts.size(); i++) {
            keysWeights[i] = (double) keyCosts.get(i) / partitionCostRecomputed;
        }

        double[] partitionAvailabilities = new double[(int)partitions];
        Arrays.fill(partitionAvailabilities, 1);
        Map<K, List<PartitionInfo>> keyPartitionProbabilities = new HashMap<>();
        for (int i = 0; i < keyCosts.size(); i++) {
            double keyPartition = keysWeights[i];
            List<PartitionInfo> list = new ArrayList<>();
            for (int j = 0; j < partitionAvailabilities.length; j++) {
                if (keyPartition == 0)
                    break;

                double partitionAvailability = partitionAvailabilities[j];
                if (partitionAvailability == 0)
                    continue;
                if (keyPartition >= partitionAvailability) {
                    list.add(new PartitionInfo(j, partitionAvailability));
                    partitionAvailabilities[j] = 0;
                    keyPartition -= partitionAvailability;
                } else {
                    list.add(new PartitionInfo(j, keyPartition));
                    partitionAvailabilities[j] -= keyPartition;
                    keyPartition = 0;
                }
            }
            if (!list.isEmpty()) {//last element might be left out due to double precision loss
                keyPartitionProbabilities.put(keys.get(i), list);
            } else {
                keyPartitionProbabilities.put(keys.get(i), Collections.singletonList(new PartitionInfo(partitionAvailabilities.length - 1, 1d)));
            }
        }

        Map<K, PartitionDistribution> distributionMap = new HashMap<>();
        Set<K> uuids = keyPartitionProbabilities.keySet();

        for (K uuid : uuids) {
            List<PartitionInfo> list = keyPartitionProbabilities.get(uuid);
            //if we have 1 partition there is no need to build a heavyweight EnumeratedDistribution
            if (list.size() == 1) {
                distributionMap.put(uuid, new PartitionDistribution(list.get(0).getPartition()));
            } else {
                List<Pair<Integer, Double>> pairs = list.stream().map(t -> new Pair<>(t.getPartition(), t.getProbability())).collect(Collectors.toList());
                distributionMap.put(uuid, new PartitionDistribution(pairs));
            }
        }
        return new PartitioningInfo<>((int)partitions, distributionMap);

    }

    @Override
    public int numPartitions() {
        return partitions;
    }

    @Override
    public int getPartition(Object partitionKey) {
        K key = (K) partitionKey;
        PartitionDistribution partitionDistribution = distribution.get(key);
        if (partitionDistribution != null) {
            return partitionDistribution.getPartition();
        } else {
            if (reservedPartitions == 0) {
                throw new RuntimeException("no reserved partitions are allocated, can't repartition the key " + partitionKey);
            }
            return (partitions - reservedPartitions) + key.hashCode() % reservedPartitions;
        }
    }

    public Map<K, PartitionDistribution> getDistribution() {
        return Collections.unmodifiableMap(distribution);
    }

    public static void main(String[] args) {
        Map<String, Long> freqs = new HashMap<>();
        String[] c = new String[]{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "a", "b", "c", "d", "e", "f"};
        String[] e = new String[26];
        for (int i = 0; i < e.length; i++) {
            e[i] = "" + (char)('a' + i);
        }
        for (int i = 0; i < c.length; i++) {
            for (int j = 0; j < e.length; j++) {
                String key = c[i]+"_"+e[j];
                freqs.put(key, (long)(500_000 + Math.random()*1_000_000));
            }
        }
        for (int i = 0; i < c.length; i++) {
            freqs.put(c[i]+"_a", (long)(5_000_000 + Math.random()*10_000_000));
            freqs.put(c[i]+"_b", (long)(2_000_000 + Math.random()*2_000_000));
        }

        CostBasedPartitioner<String, byte[]> costBasedPartitioner = new CostBasedPartitioner<>(freqs,8,500000);
        Map<String, PartitionDistribution> distributionMap = costBasedPartitioner.getDistribution();
        Set<String> k = distributionMap.keySet();
        for (Iterator iterator = k.iterator(); iterator.hasNext(); ) {
            String next = (String) iterator.next();
            System.out.println(next + " : "+ distributionMap.get(next).getEnumeratedDistribution().getPmf().size());
        }
        //costBasedPartitioner.getDistribution();
        for (int i = 0; i < c.length; i++) {
            for (int j = 0; j < e.length; j++) {
                String key = c[i]+"_"+e[j];
                int partition = costBasedPartitioner.getPartition(key);
                System.out.println(key+":"+partition);
            }
        }
        System.out.println("done");

    }
}
