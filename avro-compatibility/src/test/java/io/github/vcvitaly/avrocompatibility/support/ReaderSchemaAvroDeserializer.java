package io.github.vcvitaly.avrocompatibility.support;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

/**
 * Deserializes Confluent-framed Avro into a fixed reader schema, the "version" a consumer was built with.
 * <p>
 * A plain {@link KafkaAvroDeserializer} returning {@link GenericRecord} hands back the writer's schema,
 * so it never exercises resolution. Generated SpecificRecord classes would pin the reader schema, but
 * then two versions of one record could not live in the same test. This wrapper pins it explicitly.
 */
public class ReaderSchemaAvroDeserializer implements Deserializer<GenericRecord> {

    private final KafkaAvroDeserializer delegate;
    private final Schema readerSchema;

    public ReaderSchemaAvroDeserializer(SchemaRegistryClient registryClient, String registryUrl, Schema readerSchema) {
        this.delegate = new KafkaAvroDeserializer(registryClient, Map.of("schema.registry.url", registryUrl));
        this.readerSchema = readerSchema;
    }

    @Override
    public GenericRecord deserialize(String topic, byte[] data) {
        return (GenericRecord) delegate.deserialize(topic, data, readerSchema);
    }

    @Override
    public void close() {
        delegate.close();
    }
}
