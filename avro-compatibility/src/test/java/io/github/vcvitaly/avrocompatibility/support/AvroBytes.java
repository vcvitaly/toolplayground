package io.github.vcvitaly.avrocompatibility.support;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * Plain Avro binary encoding with no Kafka or Schema Registry involved.
 * The bytes carry no schema, so the reader must be told which schema wrote them.
 */
public final class AvroBytes {

    private AvroBytes() {
    }

    /** Encodes the record with its own schema, which acts as the writer schema. */
    public static byte[] write(GenericRecord record) {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
            new GenericDatumWriter<GenericRecord>(record.getSchema()).write(record, encoder);
            encoder.flush();
            return out.toByteArray();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** Decodes bytes written with {@code writer} into the shape of {@code reader} (schema resolution). */
    public static GenericRecord read(byte[] bytes, Schema writer, Schema reader) {
        try {
            return new GenericDatumReader<GenericRecord>(writer, reader)
                    .read(null, DecoderFactory.get().binaryDecoder(bytes, null));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
