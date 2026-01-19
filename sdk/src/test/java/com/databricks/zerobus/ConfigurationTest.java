package com.databricks.zerobus;

import static org.junit.jupiter.api.Assertions.*;

import com.databricks.test.table.TestTableRow.CityPopulationTableRow;
import com.databricks.zerobus.stream.ZerobusStream;
import org.junit.jupiter.api.Test;

/** Tests for stream configuration and options. */
public class ConfigurationTest extends BaseZerobusTest {

  @Test
  public void testStreamConfigPreserved() throws Exception {
    mockedGrpcServer.injectAckRecord(0);

    TableProperties<CityPopulationTableRow> tableProperties =
        new TableProperties<>("test.schema.table", CityPopulationTableRow.getDefaultInstance());
    StreamConfigurationOptions options =
        StreamConfigurationOptions.builder().setRecovery(true).setMaxInflightRequests(500).build();

    ZerobusStream<CityPopulationTableRow> stream =
        zerobusSdk.createStream(tableProperties, "client-id", "client-secret", options).get();

    assertEquals("test.schema.table", stream.getLegacyTableProperties().getTableName());
    assertEquals("client-id", stream.getClientId());
    assertEquals("client-secret", stream.getClientSecret());
    assertTrue(stream.getOptions().recovery());
    assertEquals(500, stream.getOptions().maxInflightRequests());
    assertNotNull(stream.getHeadersProvider());

    stream.close();
  }

  @Test
  public void testDefaultMaxInflightRecords() {
    StreamConfigurationOptions options = StreamConfigurationOptions.builder().build();

    // Default should be a reasonable value (e.g., 10000)
    assertTrue(options.maxInflightRequests() > 0);
  }

  @Test
  public void testCustomMaxInflightRecords() {
    StreamConfigurationOptions options =
        StreamConfigurationOptions.builder().setMaxInflightRequests(5000).build();

    assertEquals(5000, options.maxInflightRequests());
  }

  @Test
  public void testRecoveryOption() {
    StreamConfigurationOptions withRecovery =
        StreamConfigurationOptions.builder().setRecovery(true).build();

    StreamConfigurationOptions withoutRecovery =
        StreamConfigurationOptions.builder().setRecovery(false).build();

    assertTrue(withRecovery.recovery());
    assertFalse(withoutRecovery.recovery());
  }

  @Test
  public void testTablePropertiesGetters() {
    TableProperties<CityPopulationTableRow> tableProperties =
        new TableProperties<>("catalog.schema.table", CityPopulationTableRow.getDefaultInstance());

    assertEquals("catalog.schema.table", tableProperties.getTableName());
    assertNotNull(tableProperties.getDefaultInstance());
  }
}
