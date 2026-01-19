package com.databricks.zerobus;

import static org.junit.jupiter.api.Assertions.*;

import com.databricks.test.table.TestTableRow.CityPopulationTableRow;
import com.databricks.zerobus.batch.proto.MessageBatch;
import com.databricks.zerobus.stream.ProtoZerobusStream;
import org.junit.jupiter.api.Test;

/** Tests for error handling: null inputs and state violations using fluent API. */
public class ErrorHandlingTest extends BaseZerobusTest {

  @Test
  public void testIngestNullRecordThrows() throws Exception {
    ProtoZerobusStream<CityPopulationTableRow> stream =
        zerobusSdk
            .streamBuilder("test-table")
            .clientCredentials("client-id", "client-secret")
            .recovery(false)
            .compiledProto(CityPopulationTableRow.getDefaultInstance())
            .build()
            .get();

    assertThrows(ZerobusException.class, () -> stream.ingest((CityPopulationTableRow) null));
    stream.close();
  }

  @Test
  public void testIngestNullBatchThrows() throws Exception {
    ProtoZerobusStream<CityPopulationTableRow> stream =
        zerobusSdk
            .streamBuilder("test-table")
            .clientCredentials("client-id", "client-secret")
            .recovery(false)
            .compiledProto(CityPopulationTableRow.getDefaultInstance())
            .build()
            .get();

    assertThrows(
        ZerobusException.class,
        () -> stream.ingestBatch((MessageBatch<CityPopulationTableRow>) null));
    stream.close();
  }

  @Test
  public void testIngestAfterCloseThrows() throws Exception {
    mockedGrpcServer.injectAckRecord(0);

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

    CityPopulationTableRow record =
        CityPopulationTableRow.newBuilder().setCityName("test-city").setPopulation(1000).build();

    assertThrows(ZerobusException.class, () -> stream.ingest(record));
  }
}
