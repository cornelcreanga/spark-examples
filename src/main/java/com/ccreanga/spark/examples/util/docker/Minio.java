package com.ccreanga.spark.examples.util.docker;

import org.testcontainers.containers.MinIOContainer;
import org.testcontainers.containers.wait.strategy.Wait;

public class Minio {

    static final String DEFAULT_IMAGE = "minio/minio:RELEASE.2024-04-06T05-26-02Z";
    static final String HEALTH_ENDPOINT = "/minio/health/ready";
    public static final String MINIO_USER = "admin";
    public static final String MINIO_PASSWORD = "password";

    MinIOContainer container;

    public Minio() {

    }

    public void start() {
        container = new MinIOContainer(DEFAULT_IMAGE)
                .waitingFor(Wait.forHttp(HEALTH_ENDPOINT))
                .withExposedPorts(9000, 9001)
                .withAccessToHost(true)
                .withEnv("MINIO_ROOT_USER", MINIO_USER)
                .withEnv("MINIO_ROOT_PASSWORD", MINIO_PASSWORD)
                .withUserName(MINIO_USER)
                .withPassword(MINIO_PASSWORD);
        container.start();

    }

    public int getS3Port() {
        return container.getMappedPort(9000);
    }

    public int getUiPort() {
        return container.getMappedPort(9001);
    }

    public String getHost() {
        return container.getHost();
    }

    public void stop() {
        if (container != null) {
            container.stop();
        }
    }

}
