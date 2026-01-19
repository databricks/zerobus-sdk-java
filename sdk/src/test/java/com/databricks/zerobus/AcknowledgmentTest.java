package com.databricks.zerobus;

import static org.junit.jupiter.api.Assertions.*;

import com.databricks.test.table.TestTableRow.CityPopulationTableRow;
import com.databricks.zerobus.stream.ProtoZerobusStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.LongConsumer;
import org.junit.jupiter.api.Test;

/** Tests for acknowledgment callbacks using the fluent API. */
public class AcknowledgmentTest extends BaseZerobusTest {

  @Test
  public void testAckCallback() throws Exception {
    List<Long> ackedOffsets = Collections.synchronizedList(new ArrayList<>());
    LongConsumer ackCallback = ackedOffsets::add;

    int numRecords = 10;
    for (int i = 0; i < numRecords; i++) {
      mockedGrpcServer.injectAckRecord(i);
    }

    ProtoZerobusStream<CityPopulationTableRow> stream =
        zerobusSdk
            .streamBuilder("test-table")
            .clientCredentials("client-id", "client-secret")
            .recovery(false)
            .offsetCallback(ackCallback)
            .compiledProto(CityPopulationTableRow.getDefaultInstance())
            .build()
            .get();

    for (int i = 0; i < numRecords; i++) {
      stream.ingest(
          CityPopulationTableRow.newBuilder()
              .setCityName("test-city-" + i)
              .setPopulation(i)
              .build());
    }

    stream.flush();

    // Wait for callbacks to complete
    long deadline = System.currentTimeMillis() + 2000;
    boolean foundFinalOffset = false;
    while (System.currentTimeMillis() < deadline) {
      synchronized (ackedOffsets) {
        if (!ackedOffsets.isEmpty() && ackedOffsets.contains((long) (numRecords - 1))) {
          foundFinalOffset = true;
          break;
        }
      }
      Thread.sleep(10);
    }

    assertTrue(foundFinalOffset, "Expected to receive ack for final offset " + (numRecords - 1));
    assertTrue(ackedOffsets.size() > 0, "Expected callback to be called at least once");

    Iterator<CityPopulationTableRow> unackedRecords = stream.getUnackedRecords();
    assertFalse(unackedRecords.hasNext());

    stream.close();
  }

  @Test
  public void testCallbackExceptionHandling() throws Exception {
    List<Long> callbackInvocations = Collections.synchronizedList(new ArrayList<>());
    List<String> thrownExceptions = Collections.synchronizedList(new ArrayList<>());

    LongConsumer ackCallback =
        offsetId -> {
          callbackInvocations.add(offsetId);
          if (offsetId == 1) {
            RuntimeException exception =
                new RuntimeException("Test exception in callback for offset " + offsetId);
            thrownExceptions.add(exception.getMessage());
            throw exception;
          }
        };

    int numRecords = 3;
    for (int i = 0; i < numRecords; i++) {
      mockedGrpcServer.injectAckRecord(i);
    }

    ProtoZerobusStream<CityPopulationTableRow> stream =
        zerobusSdk
            .streamBuilder("test-table")
            .clientCredentials("client-id", "client-secret")
            .recovery(false)
            .offsetCallback(ackCallback)
            .compiledProto(CityPopulationTableRow.getDefaultInstance())
            .build()
            .get();

    for (int i = 0; i < numRecords; i++) {
      stream.ingest(
          CityPopulationTableRow.newBuilder()
              .setCityName("error-callback-device-" + i)
              .setPopulation(30 + i)
              .build());
    }

    stream.flush();

    // Wait for callbacks to complete
    long deadline = System.currentTimeMillis() + 1000;
    while (callbackInvocations.size() < numRecords && System.currentTimeMillis() < deadline) {
      Thread.sleep(10);
    }

    assertEquals(numRecords, callbackInvocations.size());
    assertTrue(callbackInvocations.contains(0L));
    assertTrue(callbackInvocations.contains(1L));
    assertTrue(callbackInvocations.contains(2L));

    assertEquals(1, thrownExceptions.size());
    assertTrue(thrownExceptions.get(0).contains("Test exception in callback for offset 1"));

    // Stream should remain functional
    Iterator<CityPopulationTableRow> unackedRecords = stream.getUnackedRecords();
    assertFalse(unackedRecords.hasNext());
    assertEquals(StreamState.OPENED, stream.getState());

    stream.close();
  }

  @Test
  public void testIngestReturnsOffsetAndWaitForOffset() throws Exception {
    mockedGrpcServer.injectAckRecord(0);

    ProtoZerobusStream<CityPopulationTableRow> stream =
        zerobusSdk
            .streamBuilder("test-table")
            .clientCredentials("client-id", "client-secret")
            .recovery(false)
            .compiledProto(CityPopulationTableRow.getDefaultInstance())
            .build()
            .get();

    // ingest returns offset ID
    long offset =
        stream.ingest(
            CityPopulationTableRow.newBuilder().setCityName("city").setPopulation(100).build());

    // waitForOffset blocks until acknowledged
    stream.waitForOffset(offset);

    // After waitForOffset completes, record should be acknowledged
    Iterator<CityPopulationTableRow> unackedRecords = stream.getUnackedRecords();
    assertFalse(unackedRecords.hasNext());

    stream.close();
  }
}
