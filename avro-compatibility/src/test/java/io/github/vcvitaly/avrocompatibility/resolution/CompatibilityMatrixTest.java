package io.github.vcvitaly.avrocompatibility.resolution;

import io.github.vcvitaly.avrocompatibility.schema.CustomerSchema;
import org.apache.avro.Schema;
import org.apache.avro.SchemaCompatibility;
import org.apache.avro.SchemaCompatibility.SchemaCompatibilityType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static io.github.vcvitaly.avrocompatibility.schema.CustomerSchema.V1;
import static io.github.vcvitaly.avrocompatibility.schema.CustomerSchema.V1_TIER_ENUM_DEFAULT;
import static io.github.vcvitaly.avrocompatibility.schema.CustomerSchema.V2_ADD_EMAIL_NO_DEFAULT;
import static io.github.vcvitaly.avrocompatibility.schema.CustomerSchema.V2_ADD_EMAIL_WITH_DEFAULT;
import static io.github.vcvitaly.avrocompatibility.schema.CustomerSchema.V2_ADD_TIER_SYMBOL;
import static io.github.vcvitaly.avrocompatibility.schema.CustomerSchema.V2_REMOVE_NAME_NO_DEFAULT;
import static io.github.vcvitaly.avrocompatibility.schema.CustomerSchema.V2_RENAME_NAME_ALIAS;
import static io.github.vcvitaly.avrocompatibility.schema.CustomerSchema.V2_WIDEN_ID_INT_TO_LONG;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The whole compatibility table, checked statically with {@link SchemaCompatibility}
 * (no bytes are written). Schema Registry runs the same kind of check when a schema is registered.
 *
 * <pre>
 *   backward = new schema can read old data   -> check(reader = new, writer = old)
 *   forward  = old schema can read new data   -> check(reader = old, writer = new)
 *   full     = both
 * </pre>
 */
class CompatibilityMatrixTest {

    record Change(String description, CustomerSchema oldSchema, CustomerSchema newSchema,
                  boolean backward, boolean forward) {
        @Override
        public String toString() {
            return description + " -> backward=" + backward + ", forward=" + forward;
        }
    }

    static Stream<Change> changes() {
        return Stream.of(
                new Change("add field with default", V1, V2_ADD_EMAIL_WITH_DEFAULT, true, true),
                new Change("add field without default", V1, V2_ADD_EMAIL_NO_DEFAULT, false, true),
                new Change("remove field that has no default", V1, V2_REMOVE_NAME_NO_DEFAULT, true, false),
                new Change("rename field, alias on new side", V1, V2_RENAME_NAME_ALIAS, true, false),
                new Change("widen int to long", V1, V2_WIDEN_ID_INT_TO_LONG, true, false),
                new Change("add enum symbol", V1, V2_ADD_TIER_SYMBOL, true, false),
                new Change("add enum symbol, old reader has enum default", V1_TIER_ENUM_DEFAULT, V2_ADD_TIER_SYMBOL, true, true)
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("changes")
    void backwardCompatibility(Change change) {
        assertThat(canRead(change.newSchema().schema(), change.oldSchema().schema()))
                .as("new reader %s, old writer %s", change.newSchema().fileName(), change.oldSchema().fileName())
                .isEqualTo(change.backward());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("changes")
    void forwardCompatibility(Change change) {
        assertThat(canRead(change.oldSchema().schema(), change.newSchema().schema()))
                .as("old reader %s, new writer %s", change.oldSchema().fileName(), change.newSchema().fileName())
                .isEqualTo(change.forward());
    }

    private static boolean canRead(Schema reader, Schema writer) {
        return SchemaCompatibility.checkReaderWriterCompatibility(reader, writer).getType()
                == SchemaCompatibilityType.COMPATIBLE;
    }
}
