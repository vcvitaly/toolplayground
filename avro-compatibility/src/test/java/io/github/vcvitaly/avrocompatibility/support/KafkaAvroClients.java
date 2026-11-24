package io.github.vcvitaly.avrocompatibility.support;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

/**
 * Producers and consumers built per schema version, so each test states which version is deployed where.
 */
public class KafkaAvroClients {

    private final String bootstrapServers;
    private final String registryUrl;
    private final SchemaRegistryClient registryClient;

    public KafkaAvroClients(String bootstrapServers, String registryUrl, SchemaRegistryClient registryClient) {
        this.bootstrapServers = bootstrapServers;
        this.registryUrl = registryUrl;
        this.registryClient = registryClient;
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
                producer.send(new ProducerRecord<>(topic, String.valueOf(record.get("id")), record)).get();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        } catch (ExecutionException e) {
            throw new IllegalStateException(e.getCause());
        }
    }

    /** Reads {@code expectedCount} records from the start of the topic, decoded into {@code readerSchema}. */
    public List<GenericRecord> consume(String topic, Schema readerSchema, int expectedCount) {
        Map<String, Object> config = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ConsumerConfig.GROUP_ID_CONFIG, "test-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"
        );
        List<GenericRecord> received = new ArrayList<>();
        try (KafkaConsumer<String, GenericRecord> consumer = new KafkaConsumer<>(config, new StringDeserializer(),
                new ReaderSchemaAvroDeserializer(registryClient, registryUrl, readerSchema))) {
            consumer.subscribe(List.of(topic));
            Instant deadline = Instant.now().plusSeconds(30);
            while (received.size() < expectedCount && Instant.now().isBefore(deadline)) {
                for (ConsumerRecord<String, GenericRecord> record : consumer.poll(Duration.ofMillis(500))) {
                    received.add(record.value());
                }
            }
        }
        if (received.size() < expectedCount) {
            throw new AssertionError("Expected " + expectedCount + " records on " + topic + ", got " + received.size());
        }
        return received;
    }
}
