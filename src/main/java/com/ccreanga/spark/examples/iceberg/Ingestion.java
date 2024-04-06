package com.ccreanga.spark.examples.iceberg;

import com.ccreanga.spark.examples.util.UuidUtils;
import com.ccreanga.spark.examples.util.docker.Nessie;
import com.ccreanga.spark.examples.util.docker.Minio;
import com.ccreanga.spark.examples.util.s3.S3Utils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigParseOptions;
import com.typesafe.config.ConfigResolveOptions;
import org.apache.commons.math3.distribution.EnumeratedDistribution;
import org.apache.commons.math3.util.Pair;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.nessie.NessieCatalog;
import org.apache.iceberg.types.Types;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.*;

import static com.ccreanga.spark.examples.util.Misc.setEnv;
import static java.util.Map.of;
import static org.apache.iceberg.TableProperties.*;

public class Ingestion {

    public static final String BUCKET = "s3://test-bucket/";
    public static final String WAREHOUSE = BUCKET + "warehouse";


    public static void main(String[] args) throws Exception {
        setEnv("AWS_REGION", "us-west-1");
//        setEnv("TESTCONTAINERS_RYUK_DISABLED", "true");
        ConfigParseOptions parseOptions = ConfigParseOptions.defaults().setAllowMissing(false);
        Config config = ConfigFactory.load("ingestion.conf", parseOptions, ConfigResolveOptions.defaults());
        String appName = config.getString("name");
        boolean sparkLocalMode = config.getBoolean("sparkLocalMode");
        long items = config.getLong("items");
        int itemsPartitions = config.getInt("itemsPartitions");
        int customersNo = config.getInt("customersNo");
        double customerSkewPercentage = config.getDouble("customerSkewPercentage") / 100d;
        double customerSkewFactor = config.getDouble("customerSkewFactor");
        int sensorEventTypesNo = config.getInt("sensorEventTypesNo");
        double sensorEventTypesSkewPercentage = config.getDouble("sensorEventTypesSkewPercentage") / 100d;
        double sensorEventTypesSkewFactor = config.getDouble("sensorEventTypesSkewFactor");

        long partitionCapacity = config.getLong("cost-partitioner.partition.capacity");


        Minio minio = new Minio();
        minio.start();

        Nessie nessieCatalog = new Nessie();
        nessieCatalog.start();

        String s3Endpoint = "http://" + minio.getHost() + ":" + minio.getS3Port();
        String nessieUrl = "http://" + nessieCatalog.getHost() + ":" + nessieCatalog.getUiPort() + "/api/v1";
        S3Utils s3Utils = new S3Utils(Minio.MINIO_USER, Minio.MINIO_PASSWORD, s3Endpoint);
        s3Utils.createBucket("test-bucket");

        //debug mode - org.apache.spark.sql.execution.adaptive -OptimizeSkewInRebalancePartitions
        SparkSession.Builder builder = SparkSession.builder();
        if (sparkLocalMode) {
            builder = builder.master("local[*]");
        }
        SparkSession spark = builder
                .appName(appName)
                .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions")
                .config("spark.sql.adaptive.enabled", config.getString("spark.sql.adaptive.enabled"))
//                .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", config.getString("spark.sql.adaptive.advisoryPartitionSizeInBytes"))
                .config("spark.sql.defaultCatalog", "nessie")
                .config("spark.sql.catalog.nessie.ref", "main")
                .config("spark.sql.catalog.nessie.uri", nessieUrl)
                .config("spark.sql.catalog.nessie.warehouse", WAREHOUSE)
                .config("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
                .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog")
                .config("spark.sql.catalog.nessie.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
                .config("spark.sql.catalog.nessie.s3.endpoint", s3Endpoint)
                .config("spark.sql.catalog.nessie.s3.path-style-access", "true")
                .config("spark.sql.catalog.nessie.s3.access-key-id", Minio.MINIO_USER)
                .config("spark.sql.catalog.nessie.s3.secret-access-key", Minio.MINIO_PASSWORD)
                .getOrCreate();
        SparkContext sc = spark.sparkContext();
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sc);


        NessieCatalog catalog = new NessieCatalog();
        catalog.setConf(sc.hadoopConfiguration());
        catalog.initialize("nessie", of(
                "ref", "main",
                "io-impl", "org.apache.iceberg.aws.s3.S3FileIO",
                "uri", nessieUrl,
                "s3.endpoint", s3Endpoint,
                "s3.path-style-access", "true",
                "s3.access-key-id", "admin",
                "s3.secret-access-key", "password",
                "warehouse", WAREHOUSE)
        );

        List<Pair<byte[], Double>> customersProbabilities = new ArrayList<>();
        boolean skewed = false;
        for (int i = 0; i < customersNo; i++) {
            if (customerSkewPercentage > Math.random()) {
                customersProbabilities.add(new Pair<>(UuidUtils.asBytes(java.util.UUID.randomUUID()), customerSkewFactor));
                skewed = true;
            } else {
                customersProbabilities.add(new Pair<>(UuidUtils.asBytes(java.util.UUID.randomUUID()), 1d));
            }
        }
        if (!skewed && customerSkewPercentage>0){
            Pair<byte[], Double> pair = customersProbabilities.get(0);
            customersProbabilities.set(0, new Pair<>(pair.getKey(), customerSkewFactor));
        }
        EnumeratedDistribution<byte[]> customersDistribution = new EnumeratedDistribution<>(customersProbabilities);


        List<Pair<Integer, Double>> sensorTypeProbabilities = new ArrayList<>();
        for (int i = 0; i < sensorEventTypesNo; i++) {
            if (sensorEventTypesSkewPercentage > Math.random()) {
                sensorTypeProbabilities.add(new Pair<>(i, sensorEventTypesSkewFactor));
            } else {
                sensorTypeProbabilities.add(new Pair<>(i, 100d));
            }
        }

        EnumeratedDistribution<Integer> sensorTypesDistribution = new EnumeratedDistribution<>(sensorTypeProbabilities);


        SensorPingRddProvider sensorPingRddProvider = new SensorPingRddProvider(jsc, itemsPartitions, items, Collections.emptyMap(), customersDistribution, sensorTypesDistribution);
        JavaRDD<SensorPing> ping = sensorPingRddProvider.buildRdd();

//        Map<String, Long> count = ping.mapToPair(sensorPing -> new Tuple2<>(convertBytesToUUID(sensorPing.customerId) + " - " + sensorPing.sensorType, sensorPing)).countByKey();
//        Stream<Map.Entry<String, Long>> sorted =
//                count.entrySet().stream()
//                        .sorted(Collections.reverseOrder(Map.Entry.comparingByValue()));
//
//        List<Map.Entry<String, Long>> list = sorted.toList();


        Dataset<SensorPing> dataset = spark.createDataset(ping.rdd(), Encoders.bean(SensorPing.class));
        dataset.persist(StorageLevel.MEMORY_AND_DISK_SER());
        System.out.println("count " + dataset.count());


//        setupCatalog(catalog, true, "items_none", "data_none");
//        sc.setLocalProperty("callSite.short", "append  - none");
//        dataset.writeTo("test.items_none")
//                .option("write-format", "parquet")
//                .option("write.parquet.compression-codec", "zstd")
//                .option("fanout-enabled", "true")
//                .option("distribution-mode", "none")
//                .append();

        setupCatalog(catalog, true, "items_hash", "data_hash");
        sc.setLocalProperty("callSite.short", "append - hash");
        dataset.writeTo("test.items_hash")
                .option("write-format", "parquet")
                .option("write.parquet.compression-codec", "zstd")
                .option("fanout-enabled", "true")
                .option("distribution-mode", "hash")
                .append();

//        setupCatalog(catalog, true, "items_range", "data_range");
//        sc.setLocalProperty("callSite.short", "append  - range");
//        dataset.writeTo("test.items_range")
//                .option("write-format", "parquet")
//                .option("write.parquet.compression-codec", "zstd")
//                .option("fanout-enabled", "true")
//                .option("distribution-mode", "range")
//                .append();


        List<Tuple2<String, Long>> keys = s3Utils.keys("test-bucket", "data_none");
        System.out.println(keys.size());
        keys = s3Utils.keys("test-bucket", "data_hash");
        System.out.println(keys.size());
        System.out.println(keys);
        keys = s3Utils.keys("test-bucket", "data_range");
        System.out.println(keys.size());

//        Dataset<Row> dsCount = spark.sql("select count(*) as no,customerId,sensorType from test.items_hash group by customerId,sensorType order by no desc");
//        dsCount.show(100);

        Dataset<Row> dsCount = spark.sql("select count(*) as no,customerId from test.items_hash group by customerId order by no desc");
        dsCount.show(100);

        catalog.close();

        System.in.read();

        //ping.take(10).forEach(System.out::println);
    }

    public static void setupCatalog(NessieCatalog catalog, boolean clean, String tableName, String location) {

        Namespace namespace = Namespace.of("test");
        TableIdentifier table = TableIdentifier.parse("test." + tableName);
        if (clean) {
            catalog.dropTable(table, true);
        }

        if (!catalog.listNamespaces().contains(namespace)) {
            catalog.createNamespace(namespace);
        }
        if (!catalog.listTables(namespace).contains(table)) {
            Schema schema = new Schema(
                    Types.NestedField.optional(1, "customerId", Types.FixedType.ofLength(16)),
                    Types.NestedField.optional(2, "sensorId", Types.FixedType.ofLength(16)),
                    Types.NestedField.optional(3, "sensorType", Types.IntegerType.get()),
                    Types.NestedField.optional(4, "latitude", Types.DoubleType.get()),
                    Types.NestedField.optional(5, "longitude", Types.DoubleType.get()),
                    Types.NestedField.optional(6, "createdTimestamp", Types.TimestampType.withZone()),
                    Types.NestedField.optional(7, "receivedTimestamp", Types.TimestampType.withZone()));

            PartitionSpec partitionSpec = PartitionSpec.builderFor(schema)
                    .hour("createdTimestamp")
                    .bucket("customerId", 10)
//                    .bucket("sensorType", 10)
                    .build();

            Map<String, String> properties = new HashMap<>();
            properties.put(DEFAULT_FILE_FORMAT, "parquet");
            properties.put(PARQUET_COMPRESSION, "zstd");
            properties.put(PARQUET_COMPRESSION_LEVEL, "3");
            properties.put(WRITE_TARGET_FILE_SIZE_BYTES, "" + 512 * 1024 * 1024);
            properties.put(WRITE_DATA_LOCATION, BUCKET + location);
            properties.put(FORMAT_VERSION, "2");

            catalog.createTable(table, schema, partitionSpec, properties);
        }

    }
}

