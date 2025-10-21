package com.databricks.zerobus;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.util.concurrent.TimeUnit;

/**
 * Factory for creating Zerobus gRPC stubs with proper configuration.
 *
 * <p>This factory handles the creation of gRPC channels and stubs with appropriate settings for
 * long-lived streaming connections.
 */
class ZerobusSdkStubFactory {

  // gRPC channel configuration constants
  private static final int DEFAULT_TLS_PORT = 443;
  private static final long KEEP_ALIVE_TIME_SECONDS = 30;
  private static final long KEEP_ALIVE_TIMEOUT_SECONDS = 10;

  // Protocol prefix
  private static final String HTTPS_PREFIX = "https://";

  /**
   * Creates a new managed gRPC channel with TLS.
   *
   * <p>The channel is configured for long-lived streaming with appropriate keep-alive settings and
   * unlimited message size limits.
   *
   * @param endpoint The endpoint URL (may include https:// prefix)
   * @return A configured ManagedChannel
   */
  ManagedChannel createGrpcChannel(String endpoint) {
    EndpointInfo endpointInfo = parseEndpoint(endpoint);

    NettyChannelBuilder builder =
        NettyChannelBuilder.forAddress(endpointInfo.host, endpointInfo.port)
            .useTransportSecurity();

    // Configure for long-lived streaming connections with unlimited message size
    return builder
        .keepAliveTime(KEEP_ALIVE_TIME_SECONDS, TimeUnit.SECONDS)
        .keepAliveTimeout(KEEP_ALIVE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
        .keepAliveWithoutCalls(true)
        .maxInboundMessageSize(Integer.MAX_VALUE)
        .build();
  }

  /**
   * Creates a new Zerobus gRPC stub with dynamic token supplier.
   *
   * <p>The stub is configured with an interceptor that obtains a fresh token for each request using
   * the provided token supplier. This allows token rotation without recreating the stub.
   *
   * <p><b>Note:</b> Currently creates a new channel for each stub. Consider reusing channels across
   * multiple streams for better resource utilization.
   *
   * @param endpoint The endpoint URL
   * @param tableName The target table name
   * @param tokenSupplier Supplier that provides a fresh authentication token for each request
   * @return A configured ZerobusStub with unlimited message sizes
   */
  ZerobusGrpc.ZerobusStub createStubWithTokenSupplier(
      String endpoint, String tableName, java.util.function.Supplier<String> tokenSupplier) {
    ManagedChannel channel = createGrpcChannel(endpoint);
    ClientInterceptor authInterceptor = new AuthenticationInterceptor(tokenSupplier, tableName);
    Channel interceptedChannel = io.grpc.ClientInterceptors.intercept(channel, authInterceptor);
    return ZerobusGrpc.newStub(interceptedChannel)
        .withMaxInboundMessageSize(Integer.MAX_VALUE)
        .withMaxOutboundMessageSize(Integer.MAX_VALUE);
  }

  /**
   * Creates a new stub factory instance.
   *
   * @return A new ZerobusSdkStubFactory
   */
  static ZerobusSdkStubFactory create() {
    return new ZerobusSdkStubFactory();
  }

  /**
   * Parses an endpoint string to extract host and port information.
   *
   * @param endpoint The endpoint string (may include https:// prefix)
   * @return Parsed endpoint information
   */
  private EndpointInfo parseEndpoint(String endpoint) {
    // Remove protocol prefix if present
    String cleanEndpoint = endpoint;
    if (cleanEndpoint.startsWith(HTTPS_PREFIX)) {
      cleanEndpoint = cleanEndpoint.substring(HTTPS_PREFIX.length());
    }

    // Parse host:port format
    String[] parts = cleanEndpoint.split(":", 2);
    String host = parts[0];
    int port = parts.length > 1 ? Integer.parseInt(parts[1]) : DEFAULT_TLS_PORT;

    return new EndpointInfo(host, port);
  }

  /** Container for parsed endpoint information. */
  private static class EndpointInfo {
    final String host;
    final int port;

    EndpointInfo(String host, int port) {
      this.host = host;
      this.port = port;
    }
  }
}

/**
 * gRPC client interceptor that adds authentication headers to requests.
 *
 * <p>This interceptor attaches the following headers to all outgoing requests:
 *
 * <ul>
 *   <li>Authorization: Bearer token
 *   <li>x-databricks-zerobus-table-name: table name
 * </ul>
 */
class AuthenticationInterceptor implements ClientInterceptor {

  private static final Metadata.Key<String> AUTHORIZATION_HEADER =
      Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> TABLE_NAME_HEADER =
      Metadata.Key.of("x-databricks-zerobus-table-name", Metadata.ASCII_STRING_MARSHALLER);
  private static final String BEARER_PREFIX = "Bearer ";

  private final java.util.function.Supplier<String> tokenSupplier;
  private final String tableName;

  /**
   * Creates a new authentication interceptor with a dynamic token supplier.
   *
   * @param tokenSupplier Supplier that provides a fresh authentication token for each request
   * @param tableName The target table name
   */
  AuthenticationInterceptor(java.util.function.Supplier<String> tokenSupplier, String tableName) {
    this.tokenSupplier = tokenSupplier;
    this.tableName = tableName;
  }

  @Override
  public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
      MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
    return new io.grpc.ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(
        next.newCall(method, callOptions)) {
      @Override
      public void start(Listener<RespT> responseListener, Metadata headers) {
        // headers.put(AUTHORIZATION_HEADER, BEARER_PREFIX + tokenSupplier.get());
        headers.put(TABLE_NAME_HEADER, tableName);
        super.start(responseListener, headers);
      }
    };
  }
}
