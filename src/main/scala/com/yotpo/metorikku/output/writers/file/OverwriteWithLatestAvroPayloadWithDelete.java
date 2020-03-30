package com.yotpo.metorikku.output.writers.file;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;
import org.apache.hudi.common.util.Option;

import java.io.IOException;

public class OverwriteWithLatestAvroPayloadWithDelete extends OverwriteWithLatestAvroPayload {
    private GenericRecord record;

    public OverwriteWithLatestAvroPayloadWithDelete(GenericRecord record, Comparable orderingVal) {
        super(record, orderingVal);
        this.record = record;
    }

    public OverwriteWithLatestAvroPayloadWithDelete(Option<GenericRecord> record) {
        super(record);
    }

    private Boolean isDeleteRecord() {
        if (record == null)
            return false;
        Object deleteField = record.get("_hoodie_delete");
        return (deleteField instanceof Boolean && (Boolean) deleteField);
    }

    @Override
    public Option<IndexedRecord> getInsertValue(Schema schema) throws IOException {
        if (isDeleteRecord())
            return Option.empty();
        else
            return super.getInsertValue(schema);
    }
}
