package com.databricks.zerobus;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

import com.databricks.test.table.TestTableRow.CityPopulationTableRow;
import com.databricks.zerobus.auth.HeadersProvider;
import com.databricks.zerobus.auth.OAuthHeadersProvider;
import com.databricks.zerobus.stream.JsonZerobusStream;
import com.databricks.zerobus.stream.ProtoZerobusStream;
import com.databricks.zerobus.tls.TlsConfig;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import org.junit.jupiter.api.Test;

/** Tests for stream creation with various configurations using fluent API. */
public class StreamCreationTest extends BaseZerobusTest {

  @Test
  public void testBasicStreamCreation() throws Exception {
    ProtoZerobusStream<CityPopulationTableRow> stream =
        zerobusSdk
            .streamBuilder("test-table")
            .clientCredentials("client-id", "client-secret")
            .recovery(false)
            .compiledProto(CityPopulationTableRow.getDefaultInstance())
            .build()
            .get();

    assertEquals(StreamState.OPENED, stream.getState());
    stream.close();
    assertEquals(StreamState.CLOSED, stream.getState());
  }

  @Test
  public void testStreamIdAssigned() throws Exception {
    ProtoZerobusStream<CityPopulationTableRow> stream =
        zerobusSdk
            .streamBuilder("test-table")
            .clientCredentials("client-id", "client-secret")
            .recovery(false)
            .compiledProto(CityPopulationTableRow.getDefaultInstance())
            .build()
            .get();

    String streamId = stream.getStreamId();
    assertNotNull(streamId);
    assertFalse(streamId.isEmpty());

    stream.close();
  }

  @Test
  public void testCreateStreamWithHeadersProvider() throws Exception {
    mockedGrpcServer.injectAckRecord(0);

    HeadersProvider headersProvider =
        new OAuthHeadersProvider(
            "test.schema.table",
            "workspace-id",
            "https://test.cloud.databricks.com",
            "client-id",
            "client-secret");

    // Use unauthenticated() path with custom headers provider
    ProtoZerobusStream<CityPopulationTableRow> stream =
        zerobusSdk
            .streamBuilder("test.schema.table")
            .unauthenticated()
            .headersProvider(headersProvider)
            .recovery(false)
            .compiledProto(CityPopulationTableRow.getDefaultInstance())
            .build()
            .get();

    assertEquals(StreamState.OPENED, stream.getState());

    long offset =
        stream.ingest(
            CityPopulationTableRow.newBuilder()
                .setCityName("test-city")
                .setPopulation(1000)
                .build());
    stream.waitForOffset(offset);

    stream.close();

    verify(zerobusSdkStubFactory, times(1))
        .createStub(
            anyString(), any(HeadersProvider.class), any(TlsConfig.class), anyInt(), anyString());
  }

  @Test
  public void testCustomHeadersProvider() throws Exception {
    mockedGrpcServer.injectAckRecord(0);

    HeadersProvider customProvider =
        new HeadersProvider() {
          @Override
          public java.util.Map<String, String> getHeaders() {
            java.util.Map<String, String> headers = new java.util.HashMap<>();
            headers.put("authorization", "Bearer custom-token");
            headers.put("x-databricks-zerobus-table-name", "test.schema.table");
            headers.put("x-custom-header", "custom-value");
            return headers;
          }
        };

    // Use unauthenticated() path with custom headers provider
    ProtoZerobusStream<CityPopulationTableRow> stream =
        zerobusSdk
            .streamBuilder("test.schema.table")
            .unauthenticated()
            .headersProvider(customProvider)
            .recovery(false)
            .compiledProto(CityPopulationTableRow.getDefaultInstance())
            .build()
            .get();

    assertEquals(StreamState.OPENED, stream.getState());

    java.util.Map<String, String> headers = customProvider.getHeaders();
    assertEquals(3, headers.size());
    assertEquals("Bearer custom-token", headers.get("authorization"));
    assertEquals("test.schema.table", headers.get("x-databricks-zerobus-table-name"));
    assertEquals("custom-value", headers.get("x-custom-header"));

    stream.close();
  }

  @Test
  public void testOAuthHeadersProviderGetHeaders() throws NonRetriableException {
    OAuthHeadersProvider provider =
        new OAuthHeadersProvider(
            "catalog.schema.table",
            "workspace-id",
            "https://test.cloud.databricks.com",
            "client-id",
            "client-secret");

    java.util.Map<String, String> headers = provider.getHeaders();

    assertTrue(headers.containsKey("authorization"));
    assertTrue(headers.get("authorization").startsWith("Bearer "));
    assertEquals("fake-token-for-testing", headers.get("authorization").substring(7));

    assertTrue(headers.containsKey("x-databricks-zerobus-table-name"));
    assertEquals("catalog.schema.table", headers.get("x-databricks-zerobus-table-name"));
  }

  @Test
  public void testJsonStreamCreation() throws Exception {
    JsonZerobusStream stream =
        zerobusSdk
            .streamBuilder("test-table")
            .clientCredentials("client-id", "client-secret")
            .recovery(false)
            .json()
            .build()
            .get();

    assertEquals(StreamState.OPENED, stream.getState());
    stream.close();
    assertEquals(StreamState.CLOSED, stream.getState());
  }

  @Test
  public void testDynamicProtoStreamCreation() throws Exception {
    // Build descriptor programmatically
    DescriptorProtos.FieldDescriptorProto cityNameField =
        DescriptorProtos.FieldDescriptorProto.newBuilder()
            .setName("city_name")
            .setNumber(1)
            .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING)
            .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
            .build();

    DescriptorProtos.FieldDescriptorProto populationField =
        DescriptorProtos.FieldDescriptorProto.newBuilder()
            .setName("population")
            .setNumber(2)
            .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64)
            .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
            .build();

    DescriptorProtos.DescriptorProto messageType =
        DescriptorProtos.DescriptorProto.newBuilder()
            .setName("DynamicCityPopulation")
            .addField(cityNameField)
            .addField(populationField)
            .build();

    DescriptorProtos.FileDescriptorProto fileDescriptorProto =
        DescriptorProtos.FileDescriptorProto.newBuilder()
            .setName("dynamic_city.proto")
            .addMessageType(messageType)
            .build();

    Descriptors.FileDescriptor fileDescriptor =
        Descriptors.FileDescriptor.buildFrom(
            fileDescriptorProto, new Descriptors.FileDescriptor[] {});
    Descriptors.Descriptor descriptor =
        fileDescriptor.findMessageTypeByName("DynamicCityPopulation");

    ProtoZerobusStream<DynamicMessage> stream =
        zerobusSdk
            .streamBuilder("test-table")
            .clientCredentials("client-id", "client-secret")
            .recovery(false)
            .dynamicProto(descriptor)
            .build()
            .get();

    assertEquals(StreamState.OPENED, stream.getState());
    stream.close();
    assertEquals(StreamState.CLOSED, stream.getState());
  }
}
