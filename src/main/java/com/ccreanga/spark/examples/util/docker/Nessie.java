package com.ccreanga.spark.examples.util.docker;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

public class Nessie {

    private GenericContainer<?> container;

    public Nessie() {
    }

    public void start() {
        container = new GenericContainer<>(DockerImageName.parse("projectnessie/nessie"))
                .withEnv("quarkus.http.port", "19120")
                .withExposedPorts(19120)
                .withEnv("nessie.version.store.type", "INMEMORY");
        container.start();
    }

    public void stop() {
        if (container != null) {
            container.stop();
        }
    }

    public int getUiPort(){
        return container.getMappedPort(19120);
    }

    public String getHost(){
        return container.getHost();
    }

}
