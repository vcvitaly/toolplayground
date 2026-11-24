package io.github.vcvitaly.avrocompatibility.support;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.kafka.ConfluentKafkaContainer;

/**
 * Kafka and Schema Registry on one Docker network, started once per JVM and shared by every test class.
 * <p>
 * Started in a static block instead of with {@code @Container}: a {@code @Container static} field in an
 * abstract base is stopped after each test class, while Spring keeps the cached context pointing at it.
 */
public final class KafkaAvroContainers {

    private static final String CONFLUENT_VERSION = "8.1.7";

    private static final Network NETWORK = Network.newNetwork();

    public static final ConfluentKafkaContainer KAFKA =
            new ConfluentKafkaContainer("confluentinc/cp-kafka:" + CONFLUENT_VERSION)
                    .withNetwork(NETWORK)
                    // Extra listener so Schema Registry can reach the broker by its network alias.
                    .withListener("kafka:19092");

    public static final GenericContainer<?> SCHEMA_REGISTRY =
            new GenericContainer<>("confluentinc/cp-schema-registry:" + CONFLUENT_VERSION)
                    .withNetwork(NETWORK)
                    .withExposedPorts(8081)
                    .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
                    .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
                    .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://kafka:19092")
                    .dependsOn(KAFKA)
                    .waitingFor(Wait.forHttp("/subjects").forStatusCode(200));

    static {
        SCHEMA_REGISTRY.start();
    }

    private KafkaAvroContainers() {
    }

    public static String bootstrapServers() {
        return KAFKA.getBootstrapServers();
    }

    public static String schemaRegistryUrl() {
        return "http://" + SCHEMA_REGISTRY.getHost() + ":" + SCHEMA_REGISTRY.getMappedPort(8081);
    }
}
