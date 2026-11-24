package io.github.vcvitaly.avrocompatibility.resolution;

import io.github.vcvitaly.avrocompatibility.schema.CustomerSchema;
import io.github.vcvitaly.avrocompatibility.support.AvroBytes;
import org.apache.avro.AvroTypeException;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import static io.github.vcvitaly.avrocompatibility.support.Customers.customer;
import static io.github.vcvitaly.avrocompatibility.support.Customers.str;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Forward compatibility: an OLD reader can read data written with the NEW schema.
 * Here every payload is written with a v2 variant and read with v1.
 * <p>
 * This is what lets you upgrade producers first: consumers that have not been redeployed
 * must cope with what the new producers write.
 */
class ForwardResolutionTest {

    @Test
    void addedFieldWithDefault_isIgnoredByTheOldReader() {
        GenericRecord read = readAsV1(customer(CustomerSchema.V2_ADD_EMAIL_WITH_DEFAULT, 1, "Ada", "GOLD")
                .set("email", "ada@example.com").build());

        assertThat(read.getSchema().getField("email")).isNull();
        assertThat(str(read, "name")).isEqualTo("Ada");
    }

    @Test
    void addedFieldWithoutDefault_isAlsoIgnoredByTheOldReader() {
        // The writer always supplies the field, so the old reader just skips it.
        GenericRecord read = readAsV1(customer(CustomerSchema.V2_ADD_EMAIL_NO_DEFAULT, 1, "Ada", "GOLD")
                .set("email", "ada@example.com").build());

        assertThat(str(read, "name")).isEqualTo("Ada");
    }

    @Test
    void removedFieldWithoutDefault_isMissingForTheOldReader() {
        GenericRecord written = customer(CustomerSchema.V2_REMOVE_NAME_NO_DEFAULT, 1, null, "GOLD").build();

        assertThatThrownBy(() -> readAsV1(written))
                .isInstanceOf(AvroTypeException.class)
                .hasMessageContaining("name");
    }

    @Test
    void renamedField_isUnknownToTheOldReader() {
        // The alias lives in the new schema only; the old reader has no way to map fullName back to name.
        GenericRecord written = customer(CustomerSchema.V2_RENAME_NAME_ALIAS, 1, "Ada", "GOLD").build();

        assertThatThrownBy(() -> readAsV1(written))
                .isInstanceOf(AvroTypeException.class)
                .hasMessageContaining("name");
    }

    @Test
    void widenedLong_cannotBeNarrowedBackToInt() {
        GenericRecord written = customer(CustomerSchema.V2_WIDEN_ID_INT_TO_LONG, 1L, "Ada", "GOLD").build();

        assertThatThrownBy(() -> readAsV1(written))
                .isInstanceOf(AvroTypeException.class);
    }

    @Test
    void newEnumSymbol_failsForAnOldReaderWithoutEnumDefault() {
        GenericRecord written = customer(CustomerSchema.V2_ADD_TIER_SYMBOL, 1, "Ada", "PLATINUM").build();

        assertThatThrownBy(() -> readAsV1(written))
                .isInstanceOf(AvroTypeException.class)
                .hasMessageContaining("PLATINUM");
    }

    @Test
    void newEnumSymbol_fallsBackToTheOldReadersEnumDefault() {
        GenericRecord written = customer(CustomerSchema.V2_ADD_TIER_SYMBOL, 1, "Ada", "PLATINUM").build();

        GenericRecord read = AvroBytes.read(AvroBytes.write(written),
                written.getSchema(), CustomerSchema.V1_TIER_ENUM_DEFAULT.schema());

        assertThat(str(read, "tier")).isEqualTo("BASIC");
    }

    @Test
    void newEnumSymbol_isFineWhenTheValueIsAnOldSymbol() {
        // Resolution fails per value, not per schema: only PLATINUM records break the old reader.
        GenericRecord written = customer(CustomerSchema.V2_ADD_TIER_SYMBOL, 1, "Ada", "GOLD").build();

        assertThat(str(readAsV1(written), "tier")).isEqualTo("GOLD");
    }

    private GenericRecord readAsV1(GenericRecord written) {
        return AvroBytes.read(AvroBytes.write(written), written.getSchema(), CustomerSchema.V1.schema());
    }
}
