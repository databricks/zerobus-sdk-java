package com.databricks.zerobus;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

import com.databricks.zerobus.auth.HeadersProvider;
import com.databricks.zerobus.auth.TokenFactory;
import com.databricks.zerobus.tls.TlsConfig;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Base test class for ZerobusSdk tests.
 *
 * <p>Provides common setup and teardown for mocked gRPC server and SDK infrastructure.
 */
@ExtendWith(MockitoExtension.class)
public abstract class BaseZerobusTest {

  protected MockedGrpcServer mockedGrpcServer;
  protected ZerobusGrpc.ZerobusStub zerobusStub;
  protected ZerobusSdk zerobusSdk;
  protected ZerobusSdkStubFactory zerobusSdkStubFactory;
  protected org.mockito.MockedStatic<TokenFactory> tokenFactoryMock;
  protected io.grpc.stub.ClientCallStreamObserver<EphemeralStreamRequest> spiedStream;

  @BeforeEach
  public void setUp() {
    // Create mocked gRPC server
    mockedGrpcServer = new MockedGrpcServer();

    // Create mocked stub
    zerobusStub = mock(ZerobusGrpc.ZerobusStub.class);

    // Create spy on stub factory
    zerobusSdkStubFactory = spy(new ZerobusSdkStubFactory());

    // Mock TokenFactory to return a fake token
    tokenFactoryMock = mockStatic(TokenFactory.class);
    tokenFactoryMock
        .when(
            () ->
                TokenFactory.getZerobusToken(
                    anyString(), anyString(), anyString(), anyString(), anyString()))
        .thenReturn("fake-token-for-testing");

    // Create ZerobusSdk with mocked stub factory
    zerobusSdk =
        ZerobusSdk.builder("localhost:50051", "https://test.cloud.databricks.com")
            .stubFactory(zerobusSdkStubFactory)
            .build();

    // Configure stub factory to return our mocked stub with headers provider
    lenient()
        .doReturn(zerobusStub)
        .when(zerobusSdkStubFactory)
        .createStub(
            anyString(), any(HeadersProvider.class), any(TlsConfig.class), anyInt(), anyString());

    // Setup mocked stub's ephemeralStream behavior
    lenient()
        .doAnswer(
            invocation -> {
              @SuppressWarnings("unchecked")
              StreamObserver<EphemeralStreamResponse> ackSender =
                  (StreamObserver<EphemeralStreamResponse>) invocation.getArgument(0);

              mockedGrpcServer.initialize(ackSender);

              // Spy on the message receiver to verify cancel() is called
              spiedStream = spy(mockedGrpcServer.getMessageReceiver());
              return spiedStream;
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
}
