package com.ccreanga.spark.examples.iceberg;

import com.ccreanga.spark.examples.util.FastRandom;
import com.ccreanga.spark.examples.util.rdd.RDDGenerator;
import com.ccreanga.spark.examples.util.rdd.RecordGenerator;
import org.apache.commons.math3.distribution.EnumeratedDistribution;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SensorPingRddProvider implements Serializable {

    private final transient JavaSparkContext jsc;
    private final int partitions;
    private final long values;
    private final Map<String, Object> context;
    private final Broadcast<EnumeratedDistribution<byte[]>> customersBroadcast;
    private final Broadcast<EnumeratedDistribution<Integer>> sensorTypesBroadcast;
    private final static FastRandom fastRandom = new FastRandom();

    public static final double initLatitude = 44.481230877283856d;
    public static final double initLongitude = 26.1745387998047d;

    public SensorPingRddProvider(JavaSparkContext jsc,
                                 int partitions,
                                 long values,
                                 Map<String, Object> context,
                                 EnumeratedDistribution<byte[]> customers,
                                 EnumeratedDistribution<Integer> sensorTypes) {
        this.jsc = jsc;
        this.partitions = partitions;
        this.values = values;
        this.context = context;
        customersBroadcast = jsc.broadcast(customers);
        sensorTypesBroadcast = jsc.broadcast(sensorTypes);
    }

    public JavaRDD<SensorPing> buildRdd() {

        RecordGenerator<SensorPing> recordGenerator = (context, itemNumber) -> {
            EnumeratedDistribution<byte[]> customers = customersBroadcast.getValue();
            EnumeratedDistribution<Integer> sensorTypes = sensorTypesBroadcast.getValue();

            List<SensorPing> pingList = new ArrayList<>();
            for (int i = 0; i < itemNumber; i++) {
                pingList.add(buildTrip(customers.sample(), sensorTypes.sample()));
            }
            return pingList;
        };

        Class clazz = SensorPing.class;
        RDDGenerator<SensorPing> rdd = RDDGenerator.of(jsc.sc(),
                partitions,
                values,
                context,
                recordGenerator,
                clazz
        );

        return rdd.toJavaRDD();
    }

    private static SensorPing buildTrip(byte[] customerId, Integer sensorType) {

        byte[] sensorId = new byte[16];
        fastRandom.nextBytes(sensorId);
        double latitude = initLatitude + fastRandom.uniform(0, 0.001);
        double longitude = initLongitude + fastRandom.uniform(0, 0.001);

        long createdTimestamp =  minutesBefore(System.currentTimeMillis(), fastRandom.nextInt(1, 2));
        long receivedTimestamp = minutesBefore(createdTimestamp, fastRandom.nextInt(0, 1));

        return new SensorPing(customerId, sensorId, sensorType, latitude, longitude, createdTimestamp, receivedTimestamp);
    }

    public static long minutesBefore(long timestamp, long minutes) {
        return timestamp - Duration.ofMinutes(minutes).toMillis();
    }
}
