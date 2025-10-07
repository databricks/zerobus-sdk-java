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
 * <p>This factory handles the creation of gRPC channels and stubs with
 * appropriate settings for long-lived streaming connections.
 */
class ZerobusSdkStubFactory {

  // gRPC channel configuration constants
  private static final int DEFAULT_TLS_PORT = 443;
  private static final int DEFAULT_PLAINTEXT_PORT = 80;
  private static final long KEEP_ALIVE_TIME_SECONDS = 30;
  private static final long KEEP_ALIVE_TIMEOUT_SECONDS = 10;
  private static final int MAX_INBOUND_MESSAGE_SIZE_MB = 100;
  private static final int BYTES_PER_MB = 1024 * 1024;

  // Protocol prefixes
  private static final String HTTPS_PREFIX = "https://";
  private static final String HTTP_PREFIX = "http://";

  /**
   * Creates a new managed gRPC channel.
   *
   * <p>The channel is configured for long-lived streaming with appropriate
   * keep-alive settings and message size limits.
   *
   * @param endpoint The endpoint URL (may include protocol prefix)
   * @param useTls Whether to use TLS encryption
   * @return A configured ManagedChannel
   */
  ManagedChannel createGrpcChannel(String endpoint, boolean useTls) {
    EndpointInfo endpointInfo = parseEndpoint(endpoint, useTls);

    NettyChannelBuilder builder = NettyChannelBuilder
        .forAddress(endpointInfo.host, endpointInfo.port);

    // Configure TLS or plaintext
    if (useTls) {
      builder.useTransportSecurity();
    } else {
      builder.usePlaintext();
    }

    // Configure for long-lived streaming connections
    return builder
        .keepAliveTime(KEEP_ALIVE_TIME_SECONDS, TimeUnit.SECONDS)
        .keepAliveTimeout(KEEP_ALIVE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
        .keepAliveWithoutCalls(true)
        .maxInboundMessageSize(MAX_INBOUND_MESSAGE_SIZE_MB * BYTES_PER_MB)
        .build();
  }

  /**
   * Creates a new Zerobus gRPC stub with authentication.
   *
   * <p>The stub is configured with an interceptor that adds authentication
   * headers to all outgoing requests.
   *
   * <p><b>Note:</b> Currently creates a new channel for each stub. Consider
   * reusing channels across multiple streams for better resource utilization.
   *
   * @param endpoint The endpoint URL
   * @param useTls Whether to use TLS encryption
   * @param tableName The target table name
   * @param token Authentication token (Bearer token)
   * @return A configured ZerobusStub
   */
  ZerobusGrpc.ZerobusStub createStub(
      String endpoint,
      boolean useTls,
      String tableName,
      String token) {
    ManagedChannel channel = createGrpcChannel(endpoint, useTls);
    ClientInterceptor authInterceptor = new AuthenticationInterceptor(token, tableName);
    Channel interceptedChannel = io.grpc.ClientInterceptors.intercept(channel, authInterceptor);
    return ZerobusGrpc.newStub(interceptedChannel);
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
   * @param endpoint The endpoint string (may include protocol)
   * @param useTls Whether TLS is being used (affects default port)
   * @return Parsed endpoint information
   */
  private EndpointInfo parseEndpoint(String endpoint, boolean useTls) {
    // Remove protocol prefix if present
    String cleanEndpoint = endpoint;
    if (cleanEndpoint.startsWith(HTTPS_PREFIX)) {
      cleanEndpoint = cleanEndpoint.substring(HTTPS_PREFIX.length());
    } else if (cleanEndpoint.startsWith(HTTP_PREFIX)) {
      cleanEndpoint = cleanEndpoint.substring(HTTP_PREFIX.length());
    }

    // Split host and port
    String[] parts = cleanEndpoint.split(":", 2);
    String host = parts[0];
    int port = parts.length > 1
        ? Integer.parseInt(parts[1])
        : (useTls ? DEFAULT_TLS_PORT : DEFAULT_PLAINTEXT_PORT);

    return new EndpointInfo(host, port);
  }

  /**
   * Container for parsed endpoint information.
   */
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
 * <ul>
 *   <li>Authorization: Bearer token</li>
 *   <li>x-databricks-zerobus-table-name: table name</li>
 * </ul>
 */
class AuthenticationInterceptor implements ClientInterceptor {

  private static final Metadata.Key<String> AUTHORIZATION_HEADER =
      Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> TABLE_NAME_HEADER =
      Metadata.Key.of("x-databricks-zerobus-table-name", Metadata.ASCII_STRING_MARSHALLER);
  private static final String BEARER_PREFIX = "Bearer ";

  private final String token;
  private final String tableName;

  /**
   * Creates a new authentication interceptor.
   *
   * @param token The authentication token (without "Bearer " prefix)
   * @param tableName The target table name
   */
  AuthenticationInterceptor(String token, String tableName) {
    this.token = token;
    this.tableName = tableName;
  }

  @Override
  public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
      MethodDescriptor<ReqT, RespT> method,
      CallOptions callOptions,
      Channel next) {
    return new io.grpc.ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(
        next.newCall(method, callOptions)) {
      @Override
      public void start(Listener<RespT> responseListener, Metadata headers) {
        headers.put(AUTHORIZATION_HEADER, BEARER_PREFIX + token);
        headers.put(TABLE_NAME_HEADER, tableName);
        super.start(responseListener, headers);
      }
    };
  }
}
