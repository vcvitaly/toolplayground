package io.github.vcvitaly.avrocompatibility.schema;

import org.apache.avro.Schema;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;

/**
 * Every version of the {@code Customer} record, loaded from {@code src/main/resources/avro/customer}.
 * <p>
 * The versions all share the full name {@code io.github.vcvitaly.avro.Customer}: Avro matches
 * reader and writer records by name, so giving each version its own class or namespace would
 * stop it from being an evolution of the same record.
 */
public enum CustomerSchema {

    V1("v1"),
    V1_TIER_ENUM_DEFAULT("v1-tier-enum-default"),
    V2_ADD_EMAIL_WITH_DEFAULT("v2-add-email-with-default"),
    V2_ADD_EMAIL_NO_DEFAULT("v2-add-email-no-default"),
    V2_REMOVE_NAME_NO_DEFAULT("v2-remove-name-no-default"),
    V2_RENAME_NAME_ALIAS("v2-rename-name-alias"),
    V2_WIDEN_ID_INT_TO_LONG("v2-widen-id-int-to-long"),
    V2_ADD_TIER_SYMBOL("v2-add-tier-symbol"),
    V3_EMAIL_DROP_DEFAULT("v3-email-drop-default");

    private final String fileName;
    private final Schema schema;

    CustomerSchema(String fileName) {
        this.fileName = fileName;
        this.schema = load("/avro/customer/" + fileName + ".avsc");
    }

    public Schema schema() {
        return schema;
    }

    public String fileName() {
        return fileName;
    }

    private static Schema load(String path) {
        try (InputStream in = CustomerSchema.class.getResourceAsStream(path)) {
            if (in == null) {
                throw new IllegalStateException("Schema not found on classpath: " + path);
            }
            // A fresh parser per file: one parser refuses to define the same record name twice.
            return new Schema.Parser().parse(in);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
