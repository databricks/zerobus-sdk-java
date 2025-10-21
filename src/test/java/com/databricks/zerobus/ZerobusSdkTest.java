package com.databricks.zerobus;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

import com.databricks.test.table.TestTableRow.CityPopulationTableRow;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Test suite for ZerobusSdk with mocked gRPC server.
 *
 * <p>These tests verify the SDK's core functionality including stream creation, record ingestion,
 * acknowledgments, and flush operations without requiring a real Zerobus backend server.
 *
 * <p>Best practices followed: - Fast execution (no long sleeps or timeouts) - Clear test names
 * describing what is being tested - Proper mock setup and teardown - Testing both success and
 * failure paths - Using CompletableFutures for async operations
 */
@ExtendWith(MockitoExtension.class)
public class ZerobusSdkTest {

  private MockedGrpcServer mockedGrpcServer;
  private ZerobusGrpc.ZerobusStub zerobusStub;
  private ZerobusSdk zerobusSdk;
  private ZerobusSdkStubFactory zerobusSdkStubFactory;
  private org.mockito.MockedStatic<TokenFactory> tokenFactoryMock;

  @BeforeEach
  public void setUp() {
    // Create mocked gRPC server
    mockedGrpcServer = new MockedGrpcServer();

    // Create mocked stub
    zerobusStub = mock(ZerobusGrpc.ZerobusStub.class);

    // Create spy on stub factory
    zerobusSdkStubFactory = spy(ZerobusSdkStubFactory.create());

    // Mock TokenFactory to return a fake token
    tokenFactoryMock = mockStatic(TokenFactory.class);
    tokenFactoryMock
        .when(
            () ->
                TokenFactory.getZerobusToken(
                    anyString(), anyString(), anyString(), anyString(), anyString()))
        .thenReturn("fake-token-for-testing");

    // Create ZerobusSdk and set mocked stub factory
    zerobusSdk = new ZerobusSdk("localhost:50051", "https://test.cloud.databricks.com");
    zerobusSdk.setStubFactory(zerobusSdkStubFactory);

    // Configure stub factory to return our mocked stub with token supplier
    doReturn(zerobusStub)
        .when(zerobusSdkStubFactory)
        .createStubWithTokenSupplier(anyString(), anyString(), any());

    // Setup mocked stub's ephemeralStream behavior
    doAnswer(
            invocation -> {
              @SuppressWarnings("unchecked")
              StreamObserver<EphemeralStreamResponse> ackSender =
                  (StreamObserver<EphemeralStreamResponse>) invocation.getArgument(0);

              mockedGrpcServer.initialize(ackSender);
              return mockedGrpcServer.getMessageReceiver();
            })
        .when(zerobusStub)
        .ephemeralStream(any());
  }

  @AfterEach
  public void tearDown() {
    if (tokenFactoryMock != null) {
      tokenFactoryMock.close();
    }
    if (mockedGrpcServer != null) {
      mockedGrpcServer.destroy();
    }
    mockedGrpcServer = null;
    zerobusStub = null;
    zerobusSdk = null;
    zerobusSdkStubFactory = null;
    tokenFactoryMock = null;
  }

  @Test
  public void testSingleRecordIngestAndAcknowledgment() throws Exception {
    // Test basic ingestion: send one record and verify it's acknowledged
    mockedGrpcServer.injectAckRecord(0);

    TableProperties<CityPopulationTableRow> tableProperties =
        new TableProperties<>("test-table", CityPopulationTableRow.getDefaultInstance());
    StreamConfigurationOptions options =
        StreamConfigurationOptions.builder().setRecovery(false).build();

    ZerobusStream<CityPopulationTableRow> stream =
        zerobusSdk.createStream(tableProperties, "client-id", "client-secret", options).get();

    assertEquals(StreamState.OPENED, stream.getState());

    CompletableFuture<Void> writeCompleted =
        stream.ingestRecord(
            CityPopulationTableRow.newBuilder()
                .setCityName("test-city")
                .setPopulation(1000)
                .build());

    // Wait for acknowledgment
    writeCompleted.get(5, TimeUnit.SECONDS);

    // Verify no unacked records
    Iterator<CityPopulationTableRow> unackedRecords = stream.getUnackedRecords();
    assertFalse(unackedRecords.hasNext());

    stream.close();
    assertEquals(StreamState.CLOSED, stream.getState());
  }

  @Test
  public void testBatchIngestion() throws Exception {
    // Test ingesting multiple records in a batch
    int batchSize = 100;

    for (int i = 0; i < batchSize; i++) {
      mockedGrpcServer.injectAckRecord(i);
    }

    TableProperties<CityPopulationTableRow> tableProperties =
        new TableProperties<>("test-table", CityPopulationTableRow.getDefaultInstance());
    StreamConfigurationOptions options =
        StreamConfigurationOptions.builder().setRecovery(false).build();

    ZerobusStream<CityPopulationTableRow> stream =
        zerobusSdk.createStream(tableProperties, "client-id", "client-secret", options).get();
    assertEquals(StreamState.OPENED, stream.getState());

    // Send records
    List<CompletableFuture<Void>> futures = new ArrayList<>();
    for (int i = 0; i < batchSize; i++) {
      futures.add(
          stream.ingestRecord(
              CityPopulationTableRow.newBuilder()
                  .setCityName("city-" + i)
                  .setPopulation(1000 + i)
                  .build()));
    }

    // Wait for all acknowledgments
    for (CompletableFuture<Void> future : futures) {
      future.get(5, TimeUnit.SECONDS);
    }

    // Verify all records acknowledged
    Iterator<CityPopulationTableRow> unackedRecords = stream.getUnackedRecords();
    assertFalse(unackedRecords.hasNext());

    stream.close();
    assertEquals(StreamState.CLOSED, stream.getState());
  }

  @Test
  public void testFlushWaitsForAllAcknowledgments() throws Exception {
    // Test that flush() blocks until all inflight records are acknowledged
    int numRecords = 10;
    mockedGrpcServer.injectAckRecord(numRecords - 1);

    TableProperties<CityPopulationTableRow> tableProperties =
        new TableProperties<>("test-table", CityPopulationTableRow.getDefaultInstance());
    StreamConfigurationOptions options =
        StreamConfigurationOptions.builder().setRecovery(false).build();

    ZerobusStream<CityPopulationTableRow> stream =
        zerobusSdk.createStream(tableProperties, "client-id", "client-secret", options).get();
    assertEquals(StreamState.OPENED, stream.getState());

    // Ingest records
    for (int i = 0; i < numRecords; i++) {
      stream.ingestRecord(
          CityPopulationTableRow.newBuilder()
              .setCityName("device-" + i)
              .setPopulation(20 + i)
              .build());
    }

    // Flush should wait for all acks
    stream.flush();

    // Verify no unacked records after flush
    Iterator<CityPopulationTableRow> unackedRecords = stream.getUnackedRecords();
    assertFalse(unackedRecords.hasNext());

    stream.close();
  }

  @Test
  public void testEmptyFlushReturnsImmediately() throws Exception {
    // Test that flush() on an empty stream returns immediately
    TableProperties<CityPopulationTableRow> tableProperties =
        new TableProperties<>("test-table", CityPopulationTableRow.getDefaultInstance());
    StreamConfigurationOptions options =
        StreamConfigurationOptions.builder().setRecovery(false).build();

    ZerobusStream<CityPopulationTableRow> stream =
        zerobusSdk.createStream(tableProperties, "client-id", "client-secret", options).get();

    assertEquals(StreamState.OPENED, stream.getState());

    // Measure flush execution time
    long startTime = System.currentTimeMillis();
    stream.flush();
    long endTime = System.currentTimeMillis();
    long flushDuration = endTime - startTime;

    assertTrue(
        flushDuration < 100,
        "Expected flush to return immediately, but took " + flushDuration + "ms");

    assertEquals(StreamState.OPENED, stream.getState());
    stream.close();
  }

  @Test
  public void testAckCallback() throws Exception {
    // Test that ack callbacks are invoked for each acknowledgment
    List<Long> ackedOffsets = Collections.synchronizedList(new ArrayList<>());
    Consumer<IngestRecordResponse> ackCallback =
        response -> ackedOffsets.add(response.getDurabilityAckUpToOffset());

    int numRecords = 10;
    for (int i = 0; i < numRecords; i++) {
      mockedGrpcServer.injectAckRecord(i);
    }

    TableProperties<CityPopulationTableRow> tableProperties =
        new TableProperties<>("test-table", CityPopulationTableRow.getDefaultInstance());
    StreamConfigurationOptions options =
        StreamConfigurationOptions.builder().setRecovery(false).setAckCallback(ackCallback).build();

    ZerobusStream<CityPopulationTableRow> stream =
        zerobusSdk.createStream(tableProperties, "client-id", "client-secret", options).get();
    assertEquals(StreamState.OPENED, stream.getState());

    // Ingest records
    List<CompletableFuture<Void>> futures = new ArrayList<>();
    for (int i = 0; i < numRecords; i++) {
      futures.add(
          stream.ingestRecord(
              CityPopulationTableRow.newBuilder()
                  .setCityName("test-city-" + i)
                  .setPopulation(i)
                  .build()));
    }

    // Wait for all records to be acknowledged
    for (CompletableFuture<Void> future : futures) {
      future.get(5, TimeUnit.SECONDS);
    }

    stream.flush();
    assertEquals(StreamState.OPENED, stream.getState());

    // Wait for callbacks to complete - wait until we see the final offset (numRecords - 1)
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

    // Verify callback was called and final offset was received
    assertTrue(foundFinalOffset, "Expected to receive ack for final offset " + (numRecords - 1));
    assertTrue(ackedOffsets.size() > 0, "Expected callback to be called at least once");

    // Verify the final offset was acknowledged
    assertTrue(
        ackedOffsets.contains((long) (numRecords - 1)),
        "Expected callbacks to include offset " + (numRecords - 1));

    // Verify unacked records are empty
    Iterator<CityPopulationTableRow> unackedRecords = stream.getUnackedRecords();
    assertFalse(unackedRecords.hasNext());

    stream.close();
    assertEquals(StreamState.CLOSED, stream.getState());
  }

  @Test
  public void testCallbackExceptionHandling() throws Exception {
    // Test that exceptions in callbacks don't crash the stream
    List<Long> callbackInvocations = new ArrayList<>();
    List<String> thrownExceptions = new ArrayList<>();

    Consumer<IngestRecordResponse> ackCallback =
        response -> {
          long offsetId = response.getDurabilityAckUpToOffset();
          callbackInvocations.add(offsetId);

          // Throw exception for offset 1 to test error handling
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

    TableProperties<CityPopulationTableRow> tableProperties =
        new TableProperties<>("test-table", CityPopulationTableRow.getDefaultInstance());
    StreamConfigurationOptions options =
        StreamConfigurationOptions.builder().setRecovery(false).setAckCallback(ackCallback).build();

    ZerobusStream<CityPopulationTableRow> stream =
        zerobusSdk.createStream(tableProperties, "client-id", "client-secret", options).get();

    assertEquals(StreamState.OPENED, stream.getState());

    List<CompletableFuture<Void>> ingestResults = new ArrayList<>();
    for (int i = 0; i < numRecords; i++) {
      CompletableFuture<Void> writeCompleted =
          stream.ingestRecord(
              CityPopulationTableRow.newBuilder()
                  .setCityName("error-callback-device-" + i)
                  .setPopulation(30 + i)
                  .build());
      ingestResults.add(writeCompleted);
    }

    // Wait for all records to be acknowledged (should succeed despite callback exception)
    for (CompletableFuture<Void> future : ingestResults) {
      future.get(5, TimeUnit.SECONDS);
    }

    // Wait for callbacks to complete
    long deadline = System.currentTimeMillis() + 1000;
    while (callbackInvocations.size() < numRecords && System.currentTimeMillis() < deadline) {
      Thread.yield();
    }

    // Verify callback was invoked for all acknowledgments (including the one that threw)
    assertEquals(numRecords, callbackInvocations.size());
    assertTrue(callbackInvocations.contains(0L));
    assertTrue(callbackInvocations.contains(1L));
    assertTrue(callbackInvocations.contains(2L));

    // Verify the exception was thrown for offset 1
    assertEquals(1, thrownExceptions.size());
    assertTrue(thrownExceptions.get(0).contains("Test exception in callback for offset 1"));

    // Verify stream remains functional
    Iterator<CityPopulationTableRow> unackedRecords = stream.getUnackedRecords();
    assertFalse(unackedRecords.hasNext());
    assertEquals(StreamState.OPENED, stream.getState());

    stream.close();
    assertEquals(StreamState.CLOSED, stream.getState());
  }
}
