package com.databricks.zerobus;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

import com.databricks.test.table.TestTableRow.CityPopulationTableRow;
import com.databricks.zerobus.stream.ProtoZerobusStream;
import java.util.Iterator;
import org.junit.jupiter.api.Test;

/** Tests for stream lifecycle operations: flush, close, state transitions. */
public class StreamLifecycleTest extends BaseZerobusTest {

  @Test
  public void testFlushWaitsForAllAcknowledgments() throws Exception {
    int numRecords = 10;
    mockedGrpcServer.injectAckRecord(numRecords - 1);

    ProtoZerobusStream<CityPopulationTableRow> stream =
        zerobusSdk
            .streamBuilder("test-table")
            .clientCredentials("client-id", "client-secret")
            .recovery(false)
            .compiledProto(CityPopulationTableRow.getDefaultInstance())
            .build()
            .get();

    for (int i = 0; i < numRecords; i++) {
      stream.ingest(
          CityPopulationTableRow.newBuilder()
              .setCityName("device-" + i)
              .setPopulation(20 + i)
              .build());
    }

    stream.flush();

    Iterator<CityPopulationTableRow> unackedRecords = stream.getUnackedRecords();
    assertFalse(unackedRecords.hasNext());

    stream.close();
  }

  @Test
  public void testEmptyFlushReturnsImmediately() throws Exception {
    ProtoZerobusStream<CityPopulationTableRow> stream =
        zerobusSdk
            .streamBuilder("test-table")
            .clientCredentials("client-id", "client-secret")
            .recovery(false)
            .compiledProto(CityPopulationTableRow.getDefaultInstance())
            .build()
            .get();

    long startTime = System.currentTimeMillis();
    stream.flush();
    long flushDuration = System.currentTimeMillis() - startTime;

    assertTrue(
        flushDuration < 100,
        "Expected flush to return immediately, but took " + flushDuration + "ms");

    assertEquals(StreamState.OPENED, stream.getState());
    stream.close();
  }

  @Test
  public void testIdempotentClose() throws Exception {
    mockedGrpcServer.injectAckRecord(0);

    ProtoZerobusStream<CityPopulationTableRow> stream =
        zerobusSdk
            .streamBuilder("test-table")
            .clientCredentials("client-id", "client-secret")
            .recovery(false)
            .compiledProto(CityPopulationTableRow.getDefaultInstance())
            .build()
            .get();

    // Close multiple times - should not throw
    stream.close();
    assertEquals(StreamState.CLOSED, stream.getState());

    stream.close();
    assertEquals(StreamState.CLOSED, stream.getState());
  }

  @Test
  public void testFlushAfterCloseReturnsImmediately() throws Exception {
    ProtoZerobusStream<CityPopulationTableRow> stream =
        zerobusSdk
            .streamBuilder("test-table")
            .clientCredentials("client-id", "client-secret")
            .recovery(false)
            .compiledProto(CityPopulationTableRow.getDefaultInstance())
            .build()
            .get();

    stream.close();
    assertEquals(StreamState.CLOSED, stream.getState());

    long startTime = System.currentTimeMillis();
    stream.flush();
    long duration = System.currentTimeMillis() - startTime;

    assertTrue(duration < 100, "Expected flush to return immediately, took " + duration + "ms");
  }

  @Test
  public void testGrpcStreamIsCancelledOnClose() throws Exception {
    mockedGrpcServer.injectAckRecord(0);

    ProtoZerobusStream<CityPopulationTableRow> stream =
        zerobusSdk
            .streamBuilder("test-table")
            .clientCredentials("client-id", "client-secret")
            .recovery(false)
            .compiledProto(CityPopulationTableRow.getDefaultInstance())
            .build()
            .get();

    long offset =
        stream.ingest(
            CityPopulationTableRow.newBuilder()
                .setCityName("test-city")
                .setPopulation(1000)
                .build());
    stream.waitForOffset(offset);

    stream.close();

    verify(spiedStream, times(1)).cancel(anyString(), any());
    verify(spiedStream, times(1)).onCompleted();
  }
}
