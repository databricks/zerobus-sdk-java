package com.databricks.zerobus.stream;

import com.databricks.zerobus.EphemeralStreamRequest;
import com.databricks.zerobus.IngestRecordBatchRequest;
import com.databricks.zerobus.IngestRecordRequest;
import com.databricks.zerobus.JsonRecordBatch;
import com.databricks.zerobus.ProtoEncodedRecordBatch;
import com.google.protobuf.ByteString;
import java.util.Collections;
import java.util.List;

/**
 * Represents a batch of encoded records ready for transmission.
 *
 * <p>Single records are represented as a batch of size 1. This provides a unified internal API
 * where all ingestion goes through the same batch-based code path.
 *
 * <p>Subclasses handle the specific encoding format (protobuf or JSON).
 */
abstract class EncodedBatch {

  /** Returns the number of records in this batch. */
  abstract int size();

  /** Converts this batch to a gRPC request with the given offset ID. */
  abstract EphemeralStreamRequest toRequest(long offsetId);

  /** Creates a proto batch containing a single encoded record. */
  static ProtoEncodedBatch protoSingle(ByteString encoded) {
    return new ProtoEncodedBatch(Collections.singletonList(encoded));
  }

  /** Creates a proto batch containing multiple encoded records. */
  static ProtoEncodedBatch protoBatch(List<ByteString> encoded) {
    return new ProtoEncodedBatch(encoded);
  }

  /** Creates a JSON batch containing a single encoded record. */
  static JsonEncodedBatch jsonSingle(String json) {
    return new JsonEncodedBatch(Collections.singletonList(json));
  }

  /** Creates a JSON batch containing multiple encoded records. */
  static JsonEncodedBatch jsonBatch(List<String> json) {
    return new JsonEncodedBatch(json);
  }
}

/** Batch of protobuf-encoded records. */
final class ProtoEncodedBatch extends EncodedBatch {
  final List<ByteString> encodedRecords;

  ProtoEncodedBatch(List<ByteString> encodedRecords) {
    this.encodedRecords = encodedRecords;
  }

  @Override
  int size() {
    return encodedRecords.size();
  }

  @Override
  EphemeralStreamRequest toRequest(long offsetId) {
    if (encodedRecords.size() == 1) {
      // Single record optimization - use IngestRecordRequest
      return EphemeralStreamRequest.newBuilder()
          .setIngestRecord(
              IngestRecordRequest.newBuilder()
                  .setOffsetId(offsetId)
                  .setProtoEncodedRecord(encodedRecords.get(0))
                  .build())
          .build();
    } else {
      // Batch - use IngestRecordBatchRequest
      ProtoEncodedRecordBatch.Builder batchBuilder = ProtoEncodedRecordBatch.newBuilder();
      for (ByteString record : encodedRecords) {
        batchBuilder.addRecords(record);
      }
      return EphemeralStreamRequest.newBuilder()
          .setIngestRecordBatch(
              IngestRecordBatchRequest.newBuilder()
                  .setOffsetId(offsetId)
                  .setProtoEncodedBatch(batchBuilder.build())
                  .build())
          .build();
    }
  }
}

/** Batch of JSON-encoded records. */
final class JsonEncodedBatch extends EncodedBatch {
  final List<String> jsonRecords;

  JsonEncodedBatch(List<String> jsonRecords) {
    this.jsonRecords = jsonRecords;
  }

  @Override
  int size() {
    return jsonRecords.size();
  }

  @Override
  EphemeralStreamRequest toRequest(long offsetId) {
    if (jsonRecords.size() == 1) {
      // Single record optimization - use IngestRecordRequest
      return EphemeralStreamRequest.newBuilder()
          .setIngestRecord(
              IngestRecordRequest.newBuilder()
                  .setOffsetId(offsetId)
                  .setJsonRecord(jsonRecords.get(0))
                  .build())
          .build();
    } else {
      // Batch - use IngestRecordBatchRequest
      JsonRecordBatch.Builder batchBuilder = JsonRecordBatch.newBuilder();
      for (String record : jsonRecords) {
        batchBuilder.addRecords(record);
      }
      return EphemeralStreamRequest.newBuilder()
          .setIngestRecordBatch(
              IngestRecordBatchRequest.newBuilder().setJsonBatch(batchBuilder.build()).build())
          .build();
    }
  }
}
