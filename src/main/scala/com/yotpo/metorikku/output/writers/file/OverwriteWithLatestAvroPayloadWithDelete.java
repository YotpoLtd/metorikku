package com.yotpo.metorikku.output.writers.file;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

import java.io.IOException;
import java.util.Optional;

public class OverwriteWithLatestAvroPayloadWithDelete extends com.uber.hoodie.OverwriteWithLatestAvroPayload {
    private GenericRecord record;

    public OverwriteWithLatestAvroPayloadWithDelete(GenericRecord record, Comparable orderingVal) {
        super(record, orderingVal);
        this.record = record;
    }

    public OverwriteWithLatestAvroPayloadWithDelete(Optional<GenericRecord> record) {
        super(record);
    }

    private Boolean isDeleteRecord() {
        Object deleteField = record.get("_hoodie_delete");
        return (deleteField != null &&
                deleteField instanceof Boolean &&
                (Boolean)deleteField == true);
    }

    @Override
    public Optional<IndexedRecord> getInsertValue(Schema schema) throws IOException {
        if (isDeleteRecord())
            return Optional.empty();
        else
            return super.getInsertValue(schema);
    }
}
