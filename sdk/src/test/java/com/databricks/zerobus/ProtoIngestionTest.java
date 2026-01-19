package com.databricks.zerobus;

import static org.junit.jupiter.api.Assertions.*;

import com.databricks.test.table.TestTableRow.CityPopulationTableRow;
import com.databricks.zerobus.batch.proto.BytesBatch;
import com.databricks.zerobus.batch.proto.MessageBatch;
import com.databricks.zerobus.stream.ProtoZerobusStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Tests for protobuf record ingestion (single and batch) using the fluent API. */
public class ProtoIngestionTest extends BaseZerobusTest {

  @Test
  public void testSingleRecordIngestAndAcknowledgment() throws Exception {
    mockedGrpcServer.injectAckRecord(0);

    ProtoZerobusStream<CityPopulationTableRow> stream =
        zerobusSdk
            .streamBuilder("test-table")
            .clientCredentials("client-id", "client-secret")
            .recovery(false)
            .compiledProto(CityPopulationTableRow.getDefaultInstance())
            .build()
            .get();

    assertEquals(StreamState.OPENED, stream.getState());

    // ingest returns offset, waitForOffset blocks until ack
    long offset =
        stream.ingest(
            CityPopulationTableRow.newBuilder()
                .setCityName("test-city")
                .setPopulation(1000)
                .build());
    stream.waitForOffset(offset);

    Iterator<CityPopulationTableRow> unackedRecords = stream.getUnackedRecords();
    assertFalse(unackedRecords.hasNext());

    stream.close();
    assertEquals(StreamState.CLOSED, stream.getState());
  }

  @Test
  public void testMultipleRecordsIngestion() throws Exception {
    int batchSize = 100;

    for (int i = 0; i < batchSize; i++) {
      mockedGrpcServer.injectAckRecord(i);
    }

    ProtoZerobusStream<CityPopulationTableRow> stream =
        zerobusSdk
            .streamBuilder("test-table")
            .clientCredentials("client-id", "client-secret")
            .recovery(false)
            .compiledProto(CityPopulationTableRow.getDefaultInstance())
            .build()
            .get();

    for (int i = 0; i < batchSize; i++) {
      stream.ingest(
          CityPopulationTableRow.newBuilder()
              .setCityName("city-" + i)
              .setPopulation(1000 + i)
              .build());
    }

    // Flush waits for all records to be acknowledged
    stream.flush();

    Iterator<CityPopulationTableRow> unackedRecords = stream.getUnackedRecords();
    assertFalse(unackedRecords.hasNext());

    stream.close();
  }

  @Test
  public void testBatchIngestRecords() throws Exception {
    mockedGrpcServer.injectAckRecord(0);

    ProtoZerobusStream<CityPopulationTableRow> stream =
        zerobusSdk
            .streamBuilder("test-table")
            .clientCredentials("client-id", "client-secret")
            .recovery(false)
            .compiledProto(CityPopulationTableRow.getDefaultInstance())
            .build()
            .get();

    List<CityPopulationTableRow> batch = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      batch.add(
          CityPopulationTableRow.newBuilder()
              .setCityName("city-" + i)
              .setPopulation(1000 + i)
              .build());
    }

    // ingestBatch returns offset, waitForOffset blocks until ack
    Long offset = stream.ingestBatch(MessageBatch.of(batch));
    assertNotNull(offset);
    stream.waitForOffset(offset);

    stream.close();
  }

  @Test
  public void testEmptyBatchReturnsImmediately() throws Exception {
    ProtoZerobusStream<CityPopulationTableRow> stream =
        zerobusSdk
            .streamBuilder("test-table")
            .clientCredentials("client-id", "client-secret")
            .recovery(false)
            .compiledProto(CityPopulationTableRow.getDefaultInstance())
            .build()
            .get();

    List<CityPopulationTableRow> emptyBatch = new ArrayList<>();
    // Empty batch returns null immediately
    Long offset = stream.ingestBatch(MessageBatch.of(emptyBatch));
    assertNull(offset);

    stream.close();
  }

  @Test
  public void testMultipleRecordsAckedTogether() throws Exception {
    // Server acks up to offset, not individual offsets
    mockedGrpcServer.injectAckRecord(4);

    ProtoZerobusStream<CityPopulationTableRow> stream =
        zerobusSdk
            .streamBuilder("test-table")
            .clientCredentials("client-id", "client-secret")
            .recovery(false)
            .compiledProto(CityPopulationTableRow.getDefaultInstance())
            .build()
            .get();

    for (int i = 0; i < 5; i++) {
      stream.ingest(
          CityPopulationTableRow.newBuilder()
              .setCityName("city-" + i)
              .setPopulation(1000 + i)
              .build());
    }

    stream.flush();

    assertFalse(stream.getUnackedRecords().hasNext());
    stream.close();
  }

  // ==================== Byte Array Overload Tests ====================

  @Test
  public void testSingleBytesIngestAndAcknowledgment() throws Exception {
    mockedGrpcServer.injectAckRecord(0);

    ProtoZerobusStream<CityPopulationTableRow> stream =
        zerobusSdk
            .streamBuilder("test-table")
            .clientCredentials("client-id", "client-secret")
            .recovery(false)
            .compiledProto(CityPopulationTableRow.getDefaultInstance())
            .build()
            .get();

    byte[] recordBytes =
        CityPopulationTableRow.newBuilder()
            .setCityName("test-city")
            .setPopulation(1000)
            .build()
            .toByteArray();

    long offset = stream.ingest(recordBytes);
    stream.waitForOffset(offset);

    stream.close();
    assertEquals(StreamState.CLOSED, stream.getState());
  }

  @Test
  public void testBytesBatchIngest() throws Exception {
    mockedGrpcServer.injectAckRecord(0);

    ProtoZerobusStream<CityPopulationTableRow> stream =
        zerobusSdk
            .streamBuilder("test-table")
            .clientCredentials("client-id", "client-secret")
            .recovery(false)
            .compiledProto(CityPopulationTableRow.getDefaultInstance())
            .build()
            .get();

    List<byte[]> batch = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      batch.add(
          CityPopulationTableRow.newBuilder()
              .setCityName("city-" + i)
              .setPopulation(1000 + i)
              .build()
              .toByteArray());
    }

    Long offset = stream.ingestBatch(BytesBatch.of(batch));
    assertNotNull(offset);
    stream.waitForOffset(offset);

    stream.close();
  }

  @Test
  public void testBytesEmptyBatchReturnsNull() throws Exception {
    ProtoZerobusStream<CityPopulationTableRow> stream =
        zerobusSdk
            .streamBuilder("test-table")
            .clientCredentials("client-id", "client-secret")
            .recovery(false)
            .compiledProto(CityPopulationTableRow.getDefaultInstance())
            .build()
            .get();

    List<byte[]> emptyBatch = new ArrayList<>();
    Long offset = stream.ingestBatch(BytesBatch.of(emptyBatch));
    assertNull(offset);

    stream.close();
  }

  @Test
  public void testBytesNullRecordThrows() throws Exception {
    ProtoZerobusStream<CityPopulationTableRow> stream =
        zerobusSdk
            .streamBuilder("test-table")
            .clientCredentials("client-id", "client-secret")
            .recovery(false)
            .compiledProto(CityPopulationTableRow.getDefaultInstance())
            .build()
            .get();

    assertThrows(ZerobusException.class, () -> stream.ingest((byte[]) null));
    stream.close();
  }

  @Test
  public void testBytesNullBatchThrows() throws Exception {
    ProtoZerobusStream<CityPopulationTableRow> stream =
        zerobusSdk
            .streamBuilder("test-table")
            .clientCredentials("client-id", "client-secret")
            .recovery(false)
            .compiledProto(CityPopulationTableRow.getDefaultInstance())
            .build()
            .get();

    assertThrows(ZerobusException.class, () -> stream.ingestBatch((BytesBatch) null));
    stream.close();
  }
}
