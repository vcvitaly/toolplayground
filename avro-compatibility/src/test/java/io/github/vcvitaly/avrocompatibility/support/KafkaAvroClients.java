package io.github.vcvitaly.avrocompatibility.support;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Producers and consumers built per schema version, so each test states which version is deployed where.
 */
public class KafkaAvroClients implements AutoCloseable {

    /** Total time {@link #consume} waits for the expected records. */
    static final Duration CONSUME_TIMEOUT = Duration.ofSeconds(30);
    /** Longest single {@code poll()}; the last poll is shortened to what is left of {@link #CONSUME_TIMEOUT}. */
    static final Duration MAX_POLL_INTERVAL = Duration.ofMillis(500);

    private final String bootstrapServers;
    private final String registryUrl;
    private final SchemaRegistryClient registryClient;
    private final Admin admin;

    public KafkaAvroClients(String bootstrapServers, String registryUrl, SchemaRegistryClient registryClient) {
        this.bootstrapServers = bootstrapServers;
        this.registryUrl = registryUrl;
        this.registryClient = registryClient;
        this.admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers));
    }

    /**
     * Creates a topic with one partition. The rollout scenarios are about the order in which old and new
     * messages arrive, and Kafka only orders records within a partition.
     */
    public void createSinglePartitionTopic(String topic) {
        await(admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all());
    }

    public void deleteTopics(Collection<String> topics) {
        if (!topics.isEmpty()) {
            await(admin.deleteTopics(topics).all());
        }
    }

    /**
     * Sends records and waits for the broker to acknowledge them.
     * The writer schema is each record's own schema; the serializer registers it under {@code <topic>-value},
     * so an incompatible schema is rejected here, before anything reaches the topic.
     */
    public void produce(String topic, GenericRecord... records) {
        Map<String, Object> config = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                "schema.registry.url", registryUrl,
                "auto.register.schemas", true
        );
        KafkaAvroSerializer valueSerializer = new KafkaAvroSerializer(registryClient, config);
        try (KafkaProducer<String, Object> producer = new KafkaProducer<>(config, new StringSerializer(), valueSerializer)) {
            for (GenericRecord record : records) {
                await(producer.send(new ProducerRecord<>(topic, String.valueOf(record.get("id")), record)));
            }
        }
    }

    /** Reads {@code expectedCount} records from the start of the topic, decoded into {@code readerSchema}. */
    public List<GenericRecord> consume(String topic, Schema readerSchema, int expectedCount) {
        Map<String, Object> config = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ConsumerConfig.GROUP_ID_CONFIG, "test-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                // Throwaway readers: committing offsets would only leave consumer groups behind.
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false
        );
        List<GenericRecord> received = new ArrayList<>();
        try (KafkaConsumer<String, GenericRecord> consumer = new KafkaConsumer<>(config, new StringDeserializer(),
                new ReaderSchemaAvroDeserializer(registryClient, registryUrl, readerSchema))) {
            consumer.subscribe(List.of(topic));
            long deadline = System.nanoTime() + CONSUME_TIMEOUT.toNanos();
            long remaining;
            while (received.size() < expectedCount && (remaining = deadline - System.nanoTime()) > 0) {
                Duration poll = Duration.ofNanos(Math.min(remaining, MAX_POLL_INTERVAL.toNanos()));
                for (ConsumerRecord<String, GenericRecord> record : consumer.poll(poll)) {
                    received.add(record.value());
                }
            }
        }
        if (received.size() < expectedCount) {
            throw new AssertionError("Expected " + expectedCount + " records on " + topic + " within "
                    + CONSUME_TIMEOUT + ", got " + received.size());
        }
        return received;
    }

    @Override
    public void close() {
        admin.close();
    }

    private static <T> T await(Future<T> future) {
        try {
            return future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        } catch (ExecutionException e) {
            throw new IllegalStateException(e.getCause());
        }
    }
}
