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
 * BACKWARD rollout: upgrade consumers first, then producers.
 * <ol>
 *   <li>The topic already holds v1 messages.</li>
 *   <li>Consumers are redeployed with v2 and must read the v1 backlog.</li>
 *   <li>Producers are redeployed with v2; consumers already understand it.</li>
 * </ol>
 * Topics have a single partition, so records are consumed in the order they were produced.
 */
class BackwardRolloutIntegrationTest extends AbstractKafkaAvroIntegrationTest {

    @Test
    void upgradedConsumer_readsTheV1BacklogAndNewV2Messages() {
        String topic = newTopic();
        setCompatibility(valueSubject(topic), "BACKWARD");

        kafka.produce(topic, v1(1, "Ada"));
        // Consumers upgraded: v2 reader over the v1 backlog.
        List<GenericRecord> backlog = kafka.consume(topic, CustomerSchema.V2_ADD_EMAIL_WITH_DEFAULT.schema(), 1);
        assertThat(str(backlog.getFirst(), "email")).isEqualTo("unknown");

        // Producers upgraded: the registry accepts v2 because it is backward compatible with v1.
        kafka.produce(topic, customer(CustomerSchema.V2_ADD_EMAIL_WITH_DEFAULT, 2, "Grace", "GOLD")
                .set("email", "grace@example.com").build());

        List<GenericRecord> all = kafka.consume(topic, CustomerSchema.V2_ADD_EMAIL_WITH_DEFAULT.schema(), 2);
        assertThat(all).extracting(r -> str(r, "email")).containsExactly("unknown", "grace@example.com");
    }

    @Test
    void backwardMode_stopsAProducerFromRegisteringAFieldWithoutDefault() {
        String topic = newTopic();
        setCompatibility(valueSubject(topic), "BACKWARD");
        kafka.produce(topic, v1(1, "Ada"));

        assertProducerRejectedAsIncompatible(() -> kafka.produce(topic,
                customer(CustomerSchema.V2_ADD_EMAIL_NO_DEFAULT, 2, "Grace", "GOLD")
                        .set("email", "grace@example.com").build()));
    }

    @Test
    void withoutTheGuard_aProducerShipsV2AndTheUpgradedConsumerChokesOnTheV1Backlog() {
        // NONE is what lets this v2 in: under the registry's default (BACKWARD) the second produce is refused.
        String topic = newTopic();
        setCompatibility(valueSubject(topic), "NONE");
        kafka.produce(topic, v1(1, "Ada"));
        kafka.produce(topic, customer(CustomerSchema.V2_ADD_EMAIL_NO_DEFAULT, 2, "Grace", "GOLD")
                .set("email", "grace@example.com").build());

        // The consumer built for that v2 meets the v1 record first, which has no email and no default to use.
        assertThatThrownBy(() -> kafka.consume(topic, CustomerSchema.V2_ADD_EMAIL_NO_DEFAULT.schema(), 2))
                .isInstanceOf(RecordDeserializationException.class)
                .hasRootCauseInstanceOf(AvroTypeException.class);
    }
}
