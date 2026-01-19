package com.databricks.zerobus;

import static org.junit.jupiter.api.Assertions.*;

import com.databricks.zerobus.batch.json.MapBatch;
import com.databricks.zerobus.batch.json.StringBatch;
import com.databricks.zerobus.stream.JsonZerobusStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Tests for JSON record ingestion using the fluent API. */
public class JsonIngestionTest extends BaseZerobusTest {

  @Test
  public void testSingleJsonRecordIngestion() throws Exception {
    mockedGrpcServer.injectAckRecord(0);

    JsonZerobusStream stream =
        zerobusSdk
            .streamBuilder("test-table")
            .clientCredentials("client-id", "client-secret")
            .recovery(false)
            .json()
            .build()
            .get();

    assertEquals(StreamState.OPENED, stream.getState());

    long offset = stream.ingest("{\"city_name\": \"test-city\", \"population\": 1000}");
    stream.waitForOffset(offset);

    Iterator<Map<String, ?>> unackedRecords = stream.getUnackedRecords();
    assertFalse(unackedRecords.hasNext());

    stream.close();
    assertEquals(StreamState.CLOSED, stream.getState());
  }

  @Test
  public void testMultipleJsonRecordsIngestion() throws Exception {
    int numRecords = 10;
    for (int i = 0; i < numRecords; i++) {
      mockedGrpcServer.injectAckRecord(i);
    }

    JsonZerobusStream stream =
        zerobusSdk
            .streamBuilder("test-table")
            .clientCredentials("client-id", "client-secret")
            .recovery(false)
            .json()
            .build()
            .get();

    for (int i = 0; i < numRecords; i++) {
      stream.ingest("{\"city_name\": \"city-" + i + "\", \"population\": " + (1000 + i) + "}");
    }

    stream.flush();

    Iterator<Map<String, ?>> unackedRecords = stream.getUnackedRecords();
    assertFalse(unackedRecords.hasNext());

    stream.close();
  }

  @Test
  public void testJsonBatchIngestion() throws Exception {
    mockedGrpcServer.injectAckRecord(0);

    JsonZerobusStream stream =
        zerobusSdk
            .streamBuilder("test-table")
            .clientCredentials("client-id", "client-secret")
            .recovery(false)
            .json()
            .build()
            .get();

    List<String> batch = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      batch.add("{\"city_name\": \"city-" + i + "\", \"population\": " + (1000 + i) + "}");
    }

    Long offset = stream.ingestBatch(StringBatch.of(batch));
    assertNotNull(offset);
    stream.waitForOffset(offset);

    stream.close();
  }

  @Test
  public void testJsonEmptyBatchReturnsNull() throws Exception {
    JsonZerobusStream stream =
        zerobusSdk
            .streamBuilder("test-table")
            .clientCredentials("client-id", "client-secret")
            .recovery(false)
            .json()
            .build()
            .get();

    List<String> emptyBatch = new ArrayList<>();
    Long offset = stream.ingestBatch(StringBatch.of(emptyBatch));
    assertNull(offset);

    stream.close();
  }

  @Test
  public void testJsonNullRecordThrows() throws Exception {
    JsonZerobusStream stream =
        zerobusSdk
            .streamBuilder("test-table")
            .clientCredentials("client-id", "client-secret")
            .recovery(false)
            .json()
            .build()
            .get();

    assertThrows(ZerobusException.class, () -> stream.ingest((String) null));
    stream.close();
  }

  @Test
  public void testJsonNullBatchThrows() throws Exception {
    JsonZerobusStream stream =
        zerobusSdk
            .streamBuilder("test-table")
            .clientCredentials("client-id", "client-secret")
            .recovery(false)
            .json()
            .build()
            .get();

    assertThrows(ZerobusException.class, () -> stream.ingestBatch((StringBatch) null));
    stream.close();
  }

  // ==================== Map Overload Tests ====================

  @Test
  public void testSingleMapIngestion() throws Exception {
    mockedGrpcServer.injectAckRecord(0);

    JsonZerobusStream stream =
        zerobusSdk
            .streamBuilder("test-table")
            .clientCredentials("client-id", "client-secret")
            .recovery(false)
            .json()
            .build()
            .get();

    Map<String, Object> record = new HashMap<>();
    record.put("city_name", "test-city");
    record.put("population", 1000);

    long offset = stream.ingest(record);
    stream.waitForOffset(offset);

    Iterator<Map<String, ?>> unackedRecords = stream.getUnackedRecords();
    assertFalse(unackedRecords.hasNext());

    stream.close();
  }

  @Test
  public void testMapBatchIngestion() throws Exception {
    mockedGrpcServer.injectAckRecord(0);

    JsonZerobusStream stream =
        zerobusSdk
            .streamBuilder("test-table")
            .clientCredentials("client-id", "client-secret")
            .recovery(false)
            .json()
            .build()
            .get();

    List<Map<String, ?>> batch = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      Map<String, Object> record = new HashMap<>();
      record.put("city_name", "city-" + i);
      record.put("population", 1000 + i);
      batch.add(record);
    }

    Long offset = stream.ingestBatch(MapBatch.of(batch));
    assertNotNull(offset);
    stream.waitForOffset(offset);

    stream.close();
  }

  @Test
  public void testMapEmptyBatchReturnsNull() throws Exception {
    JsonZerobusStream stream =
        zerobusSdk
            .streamBuilder("test-table")
            .clientCredentials("client-id", "client-secret")
            .recovery(false)
            .json()
            .build()
            .get();

    List<Map<String, ?>> emptyBatch = new ArrayList<>();
    Long offset = stream.ingestBatch(MapBatch.of(emptyBatch));
    assertNull(offset);

    stream.close();
  }

  @Test
  public void testMapNullRecordThrows() throws Exception {
    JsonZerobusStream stream =
        zerobusSdk
            .streamBuilder("test-table")
            .clientCredentials("client-id", "client-secret")
            .recovery(false)
            .json()
            .build()
            .get();

    assertThrows(ZerobusException.class, () -> stream.ingest((Map<String, ?>) null));
    stream.close();
  }

  @Test
  public void testMapNullBatchThrows() throws Exception {
    JsonZerobusStream stream =
        zerobusSdk
            .streamBuilder("test-table")
            .clientCredentials("client-id", "client-secret")
            .recovery(false)
            .json()
            .build()
            .get();

    assertThrows(ZerobusException.class, () -> stream.ingestBatch((MapBatch) null));
    stream.close();
  }

  @Test
  public void testMapWithNestedValues() throws Exception {
    mockedGrpcServer.injectAckRecord(0);

    JsonZerobusStream stream =
        zerobusSdk
            .streamBuilder("test-table")
            .clientCredentials("client-id", "client-secret")
            .recovery(false)
            .json()
            .build()
            .get();

    Map<String, Object> nested = new HashMap<>();
    nested.put("lat", 40.7128);
    nested.put("lng", -74.0060);

    Map<String, Object> record = new HashMap<>();
    record.put("city_name", "New York");
    record.put("location", nested);
    record.put("tags", java.util.Arrays.asList("urban", "coastal"));

    long offset = stream.ingest(record);
    stream.waitForOffset(offset);

    stream.close();
  }
}
