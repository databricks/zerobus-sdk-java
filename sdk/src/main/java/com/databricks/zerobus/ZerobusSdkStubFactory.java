package com.databricks.zerobus;

import com.databricks.zerobus.auth.HeadersProvider;
import com.databricks.zerobus.tls.TlsConfig;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ChannelCredentials;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ClientInterceptors;
import io.grpc.ForwardingClientCall.SimpleForwardingClientCall;
import io.grpc.Grpc;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Factory for creating Zerobus gRPC stubs with proper configuration.
 *
 * <p>This factory handles the creation of gRPC channels and stubs with appropriate settings for
 * long-lived streaming connections. The channel is cached and reused across all stubs created by
 * this factory instance, avoiding the overhead of creating new connections for each stream.
 */
class ZerobusSdkStubFactory {

  // gRPC channel configuration constants
  private static final int DEFAULT_TLS_PORT = 443;
  private static final long KEEP_ALIVE_TIME_SECONDS = 30;
  private static final long KEEP_ALIVE_TIMEOUT_SECONDS = 10;

  // Protocol prefixes
  private static final String HTTPS_PREFIX = "https://";
  private static final String HTTP_PREFIX = "http://";

  // Cached channel - initialized on first use and reused for all subsequent stubs.
  // Multiple stubs can share the same channel, avoiding the overhead of creating
  // a new channel (with its own connection pool and threads) per stream.
  private final AtomicReference<ManagedChannel> cachedChannel = new AtomicReference<>(null);

  /**
   * Gets or creates the cached gRPC channel.
   *
   * <p>The channel is configured for long-lived streaming with appropriate keep-alive settings and
   * unlimited message size limits. Uses double-checked locking for thread-safe lazy initialization.
   *
   * @param endpoint The endpoint URL (may include https:// prefix)
   * @param tlsConfig The TLS configuration for secure connection
   * @return A configured ManagedChannel (cached)
   */
  ManagedChannel getOrCreateChannel(String endpoint, TlsConfig tlsConfig) {
    ManagedChannel channel = cachedChannel.get();
    if (channel != null) {
      return channel;
    }

    synchronized (this) {
      channel = cachedChannel.get();
      if (channel != null) {
        return channel;
      }

      channel = createChannel(endpoint, tlsConfig);
      cachedChannel.set(channel);
      return channel;
    }
  }

  /**
   * Creates a new Zerobus gRPC stub.
   *
   * <p>The stub is configured with an interceptor that obtains headers for each request using the
   * provided headers provider. The underlying channel is cached and reused across all stubs.
   *
   * @param endpoint The endpoint URL
   * @param headersProvider Provider that supplies headers for each request
   * @param tlsConfig The TLS configuration for secure connection
   * @param maxMessageSizeBytes Maximum outbound message size in bytes
   * @param tableName The expected table name for header validation
   * @return A configured ZerobusStub
   */
  ZerobusGrpc.ZerobusStub createStub(
      String endpoint,
      HeadersProvider headersProvider,
      TlsConfig tlsConfig,
      int maxMessageSizeBytes,
      String tableName) {
    ManagedChannel channel = getOrCreateChannel(endpoint, tlsConfig);
    ClientInterceptor authInterceptor = new HeadersProviderInterceptor(headersProvider, tableName);
    Channel interceptedChannel = ClientInterceptors.intercept(channel, authInterceptor);
    return ZerobusGrpc.newStub(interceptedChannel)
        .withMaxInboundMessageSize(Integer.MAX_VALUE)
        .withMaxOutboundMessageSize(maxMessageSizeBytes);
  }

  /**
   * Shuts down the cached channel if it exists.
   *
   * <p>This should be called when the SDK is being closed to clean up resources.
   */
  void shutdown() {
    ManagedChannel channel = cachedChannel.getAndSet(null);
    if (channel != null) {
      channel.shutdown();
    }
  }

  /**
   * Creates a new gRPC channel (internal, not cached).
   *
   * @param endpoint The endpoint URL
   * @param tlsConfig The TLS configuration
   * @return A new ManagedChannel
   */
  private ManagedChannel createChannel(String endpoint, TlsConfig tlsConfig) {
    EndpointInfo endpointInfo = parseEndpoint(endpoint);
    ChannelCredentials credentials = tlsConfig.toChannelCredentials();

    return Grpc.newChannelBuilder(endpointInfo.host + ":" + endpointInfo.port, credentials)
        .keepAliveTime(KEEP_ALIVE_TIME_SECONDS, TimeUnit.SECONDS)
        .keepAliveTimeout(KEEP_ALIVE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
        .keepAliveWithoutCalls(true)
        .maxInboundMessageSize(Integer.MAX_VALUE)
        .build();
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

  /**
   * Parses an endpoint string to extract host and port information.
   *
   * @param endpoint The endpoint string (may include https:// or http:// prefix)
   * @return Parsed endpoint information
   */
  private EndpointInfo parseEndpoint(String endpoint) {
    String cleanEndpoint = endpoint;
    if (cleanEndpoint.startsWith(HTTPS_PREFIX)) {
      cleanEndpoint = cleanEndpoint.substring(HTTPS_PREFIX.length());
    } else if (cleanEndpoint.startsWith(HTTP_PREFIX)) {
      cleanEndpoint = cleanEndpoint.substring(HTTP_PREFIX.length());
    }

    String[] parts = cleanEndpoint.split(":", 2);
    String host = parts[0];
    int port = parts.length > 1 ? Integer.parseInt(parts[1]) : DEFAULT_TLS_PORT;

    return new EndpointInfo(host, port);
  }

  static final String TABLE_NAME_HEADER = "x-databricks-zerobus-table-name";

  /**
   * Validates that the headers contain the expected table name.
   *
   * @param headers The headers from the provider
   * @param expectedTableName The expected table name
   * @throws NonRetriableException if validation fails
   */
  static void validateTableNameHeader(Map<String, String> headers, String expectedTableName)
      throws NonRetriableException {
    String headerTableName = headers.get(TABLE_NAME_HEADER);
    if (headerTableName == null) {
      throw new NonRetriableException(
          "Headers provider must include '" + TABLE_NAME_HEADER + "' header");
    }
    if (!headerTableName.equals(expectedTableName)) {
      throw new NonRetriableException(
          "Table name mismatch: headers provider returned '"
              + headerTableName
              + "' but stream is configured for '"
              + expectedTableName
              + "'");
    }
  }

  /**
   * gRPC client interceptor that adds headers from a HeadersProvider to requests.
   *
   * <p>This interceptor obtains headers from the provided HeadersProvider and attaches them to all
   * outgoing requests. It also validates that the table name header matches the expected value.
   */
  private static class HeadersProviderInterceptor implements ClientInterceptor {

    private final HeadersProvider headersProvider;
    private final String expectedTableName;

    HeadersProviderInterceptor(HeadersProvider headersProvider, String expectedTableName) {
      this.headersProvider = headersProvider;
      this.expectedTableName = expectedTableName;
    }

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
        MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
      return new SimpleForwardingClientCall<ReqT, RespT>(next.newCall(method, callOptions)) {
        @Override
        public void start(Listener<RespT> responseListener, Metadata headers) {
          try {
            Map<String, String> providerHeaders = headersProvider.getHeaders();

            validateTableNameHeader(providerHeaders, expectedTableName);

            for (Map.Entry<String, String> entry : providerHeaders.entrySet()) {
              Metadata.Key<String> key =
                  Metadata.Key.of(entry.getKey(), Metadata.ASCII_STRING_MARSHALLER);
              headers.put(key, entry.getValue());
            }

            super.start(responseListener, headers);
          } catch (NonRetriableException e) {
            throw new RuntimeException("Failed to get headers from provider", e);
          }
        }
      };
    }
  }
}
