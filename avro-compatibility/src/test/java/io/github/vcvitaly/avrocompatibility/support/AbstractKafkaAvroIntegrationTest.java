package io.github.vcvitaly.avrocompatibility.support;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.github.vcvitaly.avrocompatibility.schema.CustomerSchema;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.UUID;

/**
 * Shared Spring context for the {@code registry} and {@code kafka} packages.
 * Every test uses its own topic and subject, so tests never see each other's schemas or messages.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@Import(AbstractKafkaAvroIntegrationTest.KafkaAvroTestConfiguration.class)
public abstract class AbstractKafkaAvroIntegrationTest {

    @TestConfiguration(proxyBeanMethods = false)
    static class KafkaAvroTestConfiguration {

        @Bean
        SchemaRegistryClient schemaRegistryClient() {
            return new CachedSchemaRegistryClient(KafkaAvroContainers.schemaRegistryUrl(), 100);
        }

        @Bean
        KafkaAvroClients kafkaAvroClients(SchemaRegistryClient schemaRegistryClient) {
            return new KafkaAvroClients(KafkaAvroContainers.bootstrapServers(),
                    KafkaAvroContainers.schemaRegistryUrl(), schemaRegistryClient);
        }
    }

    @Autowired
    protected SchemaRegistryClient registry;

    @Autowired
    protected KafkaAvroClients kafka;

    protected static String uniqueTopic() {
        return "customers-" + UUID.randomUUID();
    }

    /** Subject name the default TopicNameStrategy uses for message values. */
    protected static String valueSubject(String topic) {
        return topic + "-value";
    }

    /** One of BACKWARD, FORWARD, FULL, NONE, or their _TRANSITIVE variants. */
    protected void setCompatibility(String subject, String level) {
        try {
            registry.updateCompatibility(subject, level);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (RestClientException e) {
            throw new IllegalStateException(e);
        }
    }

    protected int register(String subject, CustomerSchema version) throws IOException, RestClientException {
        return registry.register(subject, new AvroSchema(version.schema()));
    }
}
