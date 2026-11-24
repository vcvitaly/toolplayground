package io.github.vcvitaly.avrocompatibility.resolution;

import io.github.vcvitaly.avrocompatibility.schema.CustomerSchema;
import io.github.vcvitaly.avrocompatibility.support.AvroBytes;
import io.github.vcvitaly.avrocompatibility.support.Customers;
import org.apache.avro.AvroTypeException;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import static io.github.vcvitaly.avrocompatibility.support.Customers.str;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Backward compatibility: a NEW reader can read data written with the OLD schema.
 * Here every payload is written with v1 and read with a v2 variant.
 * <p>
 * This is what lets you upgrade consumers first: they still have to read what is already on the topic.
 */
class BackwardResolutionTest {

    private final byte[] v1Bytes = AvroBytes.write(Customers.v1(1, "Ada"));

    @Test
    void addedFieldWithDefault_isFilledFromTheDefault() {
        GenericRecord read = readV1As(CustomerSchema.V2_ADD_EMAIL_WITH_DEFAULT);

        assertThat(str(read, "name")).isEqualTo("Ada");
        assertThat(str(read, "email")).isEqualTo("unknown");
    }

    @Test
    void addedFieldWithoutDefault_cannotBeFilled() {
        assertThatThrownBy(() -> readV1As(CustomerSchema.V2_ADD_EMAIL_NO_DEFAULT))
                .isInstanceOf(AvroTypeException.class)
                .hasMessageContaining("email");
    }

    @Test
    void removedField_isSkippedByTheReader() {
        GenericRecord read = readV1As(CustomerSchema.V2_REMOVE_NAME_NO_DEFAULT);

        assertThat(read.getSchema().getField("name")).isNull();
        assertThat(read.get("id")).isEqualTo(1);
    }

    @Test
    void renamedField_isMatchedThroughTheReadersAlias() {
        GenericRecord read = readV1As(CustomerSchema.V2_RENAME_NAME_ALIAS);

        assertThat(str(read, "fullName")).isEqualTo("Ada");
    }

    @Test
    void widenedInt_isPromotedToLong() {
        GenericRecord read = readV1As(CustomerSchema.V2_WIDEN_ID_INT_TO_LONG);

        assertThat(read.get("id")).isEqualTo(1L);
    }

    @Test
    void addedEnumSymbol_stillReadsOldSymbols() {
        GenericRecord read = readV1As(CustomerSchema.V2_ADD_TIER_SYMBOL);

        assertThat(str(read, "tier")).isEqualTo("GOLD");
    }

    private GenericRecord readV1As(CustomerSchema newReader) {
        return AvroBytes.read(v1Bytes, CustomerSchema.V1.schema(), newReader.schema());
    }
}
