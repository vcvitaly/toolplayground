package io.github.vcvitaly.avrocompatibility.registry;

import io.github.vcvitaly.avrocompatibility.schema.CustomerSchema;
import io.github.vcvitaly.avrocompatibility.support.AbstractKafkaAvroIntegrationTest;
import org.junit.jupiter.api.Test;

import static io.github.vcvitaly.avrocompatibility.registry.CompatibilityModeIntegrationTest.assertRejected;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * v1 -> v2 adds {@code email} with a default, v3 drops the default.
 * <ul>
 *   <li>v3 can read v2 data: every v2 record has an email.</li>
 *   <li>v3 cannot read v1 data: those records have no email and v3 has no default to fill in.</li>
 * </ul>
 * BACKWARD only compares a new schema with the latest one, so it lets v3 through even though v1 records
 * may still be on the topic. BACKWARD_TRANSITIVE compares with every earlier version and refuses it.
 */
class TransitiveCompatibilityIntegrationTest extends AbstractKafkaAvroIntegrationTest {

    @Test
    void backward_checksOnlyTheLatestVersion() throws Exception {
        String subject = newSubject();
        setCompatibility(subject, "BACKWARD");

        register(subject, CustomerSchema.V1);
        register(subject, CustomerSchema.V2_ADD_EMAIL_WITH_DEFAULT);
        register(subject, CustomerSchema.V3_EMAIL_DROP_DEFAULT);

        assertThat(registry.getAllVersions(subject)).hasSize(3);
    }

    @Test
    void backwardTransitive_checksEveryEarlierVersion() throws Exception {
        String subject = newSubject();
        setCompatibility(subject, "BACKWARD_TRANSITIVE");

        register(subject, CustomerSchema.V1);
        register(subject, CustomerSchema.V2_ADD_EMAIL_WITH_DEFAULT);

        assertRejected(() -> register(subject, CustomerSchema.V3_EMAIL_DROP_DEFAULT));
        assertThat(registry.getAllVersions(subject)).hasSize(2);
    }
}
