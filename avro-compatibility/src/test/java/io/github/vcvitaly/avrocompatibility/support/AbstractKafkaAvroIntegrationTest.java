package io.github.vcvitaly.avrocompatibility.support;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.github.vcvitaly.avrocompatibility.schema.CustomerSchema;
import org.apache.kafka.common.errors.SerializationException;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.AfterEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Shared Spring context for the {@code registry} and {@code kafka} packages.
 * Every test uses its own topic and subject, so tests never see each other's schemas or messages,
 * and both are deleted after the test so the shared containers stay empty.
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

    private final List<String> topicsToDelete = new ArrayList<>();
    private final List<String> subjectsToDelete = new ArrayList<>();

    /** A fresh single-partition topic; it and its value subject are removed after the test. */
    protected String newTopic() {
        String topic = "customers-" + UUID.randomUUID();
        kafka.createSinglePartitionTopic(topic);
        topicsToDelete.add(topic);
        subjectsToDelete.add(valueSubject(topic));
        return topic;
    }

    /** A fresh subject with no topic behind it; it and its compatibility override are removed after the test. */
    protected String newSubject() {
        String subject = valueSubject("customers-" + UUID.randomUUID());
        subjectsToDelete.add(subject);
        return subject;
    }

    @AfterEach
    void deleteTopicsAndSubjects() throws IOException, RestClientException {
        kafka.deleteTopics(topicsToDelete);
        for (String subject : subjectsToDelete) {
            ignoringNotFound(() -> registry.deleteCompatibility(subject));
            // The registry only allows a permanent delete after a soft one.
            ignoringNotFound(() -> registry.deleteSubject(subject, false));
            ignoringNotFound(() -> registry.deleteSubject(subject, true));
        }
    }

    private interface RegistryCall {
        void run() throws IOException, RestClientException;
    }

    private static void ignoringNotFound(RegistryCall call) throws IOException, RestClientException {
        try {
            call.run();
        } catch (RestClientException e) {
            if (e.getStatus() != 404) {
                throw e;
            }
        }
    }

    /**
     * The producer's serializer failed because the registry refused the schema as incompatible (HTTP 409),
     * not because of some other registry error such as 401 or 500.
     */
    protected static void assertProducerRejectedAsIncompatible(ThrowingCallable produce) {
        Throwable thrown = catchThrowable(produce);
        assertThat(thrown).isInstanceOf(SerializationException.class);
        assertThat(thrown.getCause())
                .isInstanceOfSatisfying(RestClientException.class, e -> assertThat(e.getStatus()).isEqualTo(409));
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
