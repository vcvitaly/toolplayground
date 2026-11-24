package io.github.vcvitaly.avrocompatibility.kafka;

import io.github.vcvitaly.avrocompatibility.schema.CustomerSchema;
import io.github.vcvitaly.avrocompatibility.support.AbstractKafkaAvroIntegrationTest;
import org.apache.avro.AvroTypeException;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.github.vcvitaly.avrocompatibility.support.Customers.customer;
import static io.github.vcvitaly.avrocompatibility.support.Customers.str;
import static io.github.vcvitaly.avrocompatibility.support.Customers.v1;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * FORWARD rollout: upgrade producers first, then consumers.
 * <ol>
 *   <li>Producers are redeployed with v2 and start writing v2 messages.</li>
 *   <li>Consumers still run v1 and must read them.</li>
 *   <li>Consumers are redeployed later, at their own pace.</li>
 * </ol>
 * Topics have a single partition, so records are consumed in the order they were produced.
 */
class ForwardRolloutIntegrationTest extends AbstractKafkaAvroIntegrationTest {

    @Test
    void oldConsumer_readsMessagesFromAnUpgradedProducer() {
        String topic = newTopic();
        setCompatibility(valueSubject(topic), "FORWARD");

        kafka.produce(topic, v1(1, "Ada"));
        // Producer upgraded: v2 adds a required field, which FORWARD allows.
        kafka.produce(topic, customer(CustomerSchema.V2_ADD_EMAIL_NO_DEFAULT, 2, "Grace", "GOLD")
                .set("email", "grace@example.com").build());

        // Consumer not upgraded yet: v1 reader over v1 and v2 messages.
        List<GenericRecord> received = kafka.consume(topic, CustomerSchema.V1.schema(), 2);

        assertThat(received).extracting(r -> str(r, "name")).containsExactly("Ada", "Grace");
        assertThat(received).allSatisfy(r -> assertThat(r.getSchema().getField("email")).isNull());
    }

    @Test
    void forwardMode_stopsAProducerFromRemovingAFieldOldConsumersNeed() {
        String topic = newTopic();
        setCompatibility(valueSubject(topic), "FORWARD");
        kafka.produce(topic, v1(1, "Ada"));

        assertProducerRejectedAsIncompatible(() -> kafka.produce(topic,
                customer(CustomerSchema.V2_REMOVE_NAME_NO_DEFAULT, 2, null, "GOLD").build()));
    }

    @Test
    void wrongModeForTheRolloutOrder_breaksOldConsumers() {
        // BACKWARD accepts removing 'name', but that only protects consumers that upgraded first.
        // Upgrading the producer first with this schema breaks every consumer still on v1.
        String topic = newTopic();
        setCompatibility(valueSubject(topic), "BACKWARD");
        kafka.produce(topic, v1(1, "Ada"));

        kafka.produce(topic, customer(CustomerSchema.V2_REMOVE_NAME_NO_DEFAULT, 2, null, "GOLD").build());

        assertThatThrownBy(() -> kafka.consume(topic, CustomerSchema.V1.schema(), 2))
                .isInstanceOf(RecordDeserializationException.class)
                .hasRootCauseInstanceOf(AvroTypeException.class);
    }
}
