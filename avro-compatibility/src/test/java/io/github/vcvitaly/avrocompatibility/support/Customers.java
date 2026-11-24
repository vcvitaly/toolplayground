package io.github.vcvitaly.avrocompatibility.support;

import io.github.vcvitaly.avrocompatibility.schema.CustomerSchema;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

/** Builds sample {@code Customer} records for a given schema version. */
public final class Customers {

    private Customers() {
    }

    /** Starts a builder with {@code id}, {@code tier} and, where the version has one, {@code name}. */
    public static GenericRecordBuilder customer(CustomerSchema version, Number id, String name, String tier) {
        Schema schema = version.schema();
        GenericRecordBuilder builder = new GenericRecordBuilder(schema)
                .set("id", schema.getField("id").schema().getType() == Schema.Type.LONG ? id.longValue() : id.intValue())
                .set("tier", tier(schema, tier));
        if (schema.getField("name") != null) {
            builder.set("name", name);
        }
        if (schema.getField("fullName") != null) {
            builder.set("fullName", name);
        }
        return builder;
    }

    public static GenericRecord v1(int id, String name) {
        return customer(CustomerSchema.V1, id, name, "GOLD").build();
    }

    public static GenericData.EnumSymbol tier(Schema customerSchema, String symbol) {
        return new GenericData.EnumSymbol(customerSchema.getField("tier").schema(), symbol);
    }

    /** Avro decodes strings as {@code Utf8}; this reads any field back as a plain String. */
    public static String str(GenericRecord record, String field) {
        Object value = record.get(field);
        return value == null ? null : value.toString();
    }
}
