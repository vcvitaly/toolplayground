package io.github.vcvitaly.avrocompatibility.registry;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.github.vcvitaly.avrocompatibility.schema.CustomerSchema;
import io.github.vcvitaly.avrocompatibility.support.AbstractKafkaAvroIntegrationTest;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Schema Registry enforces compatibility per subject at registration time.
 * An incompatible schema is refused with HTTP 409 and never gets an id, so no producer can write with it.
 */
class CompatibilityModeIntegrationTest extends AbstractKafkaAvroIntegrationTest {

    @Test
    void backward_acceptsAddedFieldWithDefault() throws Exception {
        String subject = subjectWith("BACKWARD");

        int v2Id = register(subject, CustomerSchema.V2_ADD_EMAIL_WITH_DEFAULT);

        assertThat(registry.getAllVersions(subject)).hasSize(2);
        assertThat(v2Id).isPositive();
    }

    @Test
    void backward_rejectsAddedFieldWithoutDefault() {
        String subject = subjectWith("BACKWARD");

        assertRejected(() -> register(subject, CustomerSchema.V2_ADD_EMAIL_NO_DEFAULT));
    }

    @Test
    void forward_acceptsAddedFieldWithoutDefault() throws Exception {
        String subject = subjectWith("FORWARD");

        register(subject, CustomerSchema.V2_ADD_EMAIL_NO_DEFAULT);

        assertThat(registry.getAllVersions(subject)).hasSize(2);
    }

    @Test
    void forward_rejectsRemovedFieldWithoutDefault() {
        String subject = subjectWith("FORWARD");

        assertRejected(() -> register(subject, CustomerSchema.V2_REMOVE_NAME_NO_DEFAULT));
    }

    @Test
    void full_acceptsOnlyChangesThatAreBothBackwardAndForward() throws Exception {
        String subject = subjectWith("FULL");

        assertThat(registry.testCompatibility(subject, new AvroSchema(CustomerSchema.V2_ADD_EMAIL_WITH_DEFAULT.schema()))).isTrue();
        assertThat(registry.testCompatibility(subject, new AvroSchema(CustomerSchema.V2_ADD_EMAIL_NO_DEFAULT.schema()))).isFalse();
        assertThat(registry.testCompatibility(subject, new AvroSchema(CustomerSchema.V2_WIDEN_ID_INT_TO_LONG.schema()))).isFalse();
    }

    @Test
    void none_acceptsAnything() throws Exception {
        String subject = subjectWith("NONE");

        register(subject, CustomerSchema.V2_REMOVE_NAME_NO_DEFAULT);
        register(subject, CustomerSchema.V2_ADD_EMAIL_NO_DEFAULT);

        assertThat(registry.getAllVersions(subject)).hasSize(3);
    }

    /** Creates a fresh subject with the given compatibility level and v1 already registered. */
    private String subjectWith(String compatibility) {
        String subject = newSubject();
        setCompatibility(subject, compatibility);
        try {
            register(subject, CustomerSchema.V1);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
        return subject;
    }

    static void assertRejected(ThrowingCallable registration) {
        assertThatThrownBy(registration)
                .isInstanceOfSatisfying(RestClientException.class, e -> assertThat(e.getStatus()).isEqualTo(409));
    }
}
