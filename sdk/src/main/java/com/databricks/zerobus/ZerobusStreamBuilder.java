package com.databricks.zerobus;

import com.databricks.zerobus.auth.HeadersProvider;
import com.databricks.zerobus.auth.OAuthHeadersProvider;
import com.databricks.zerobus.schema.JsonTableProperties;
import com.databricks.zerobus.schema.ProtoTableProperties;
import com.databricks.zerobus.stream.JsonZerobusStream;
import com.databricks.zerobus.stream.ProtoZerobusStream;
import com.databricks.zerobus.stream.ZerobusStream;
import com.databricks.zerobus.tls.SecureTlsConfig;
import com.databricks.zerobus.tls.TlsConfig;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.LongConsumer;
import java.util.function.Supplier;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builder for creating Zerobus streams with a fluent API.
 *
 * <p>This builder uses a step builder pattern that enforces authentication configuration at compile
 * time. You must call either {@link #clientCredentials} (for OAuth) or {@link #unauthenticated}
 * (for custom auth) before you can access schema selection methods.
 *
 * <p>The builder flow:
 *
 * <ol>
 *   <li>{@code sdk.streamBuilder(tableName)} - returns {@code ZerobusStreamBuilder}
 *   <li>{@code .clientCredentials(...)} or {@code .unauthenticated()} - returns a configurable
 *       builder
 *   <li>Optional: configure stream options ({@code .maxInflightRequests()}, {@code
 *       .headersProvider()}, etc.)
 *   <li>{@code .compiledProto(...)} or {@code .json()} - returns typed stream builder
 *   <li>{@code .build()} - creates the stream
 * </ol>
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * // OAuth authentication (most common)
 * ProtoZerobusStream<MyRecord> stream = sdk.streamBuilder("catalog.schema.table")
 *     .clientCredentials(clientId, clientSecret)
 *     .maxInflightRequests(10000)
 *     .compiledProto(MyRecord.getDefaultInstance())
 *     .build()
 *     .join();
 *
 * // Custom authentication with headers provider
 * JsonZerobusStream stream = sdk.streamBuilder("catalog.schema.table")
 *     .unauthenticated()
 *     .headersProvider(customProvider)
 *     .json()
 *     .build()
 *     .join();
 * }</pre>
 *
 * @see ZerobusSdk#streamBuilder(String)
 */
public class ZerobusStreamBuilder {

  private static final Logger logger = LoggerFactory.getLogger(ZerobusStreamBuilder.class);

  private final ZerobusSdkStubFactory stubFactory;
  private final ExecutorService executor;
  private final String workspaceId;
  private final String serverEndpoint;
  private final String unityCatalogEndpoint;
  private final String tableName;

  /**
   * Creates a new ZerobusStreamBuilder.
   *
   * <p>Use {@link ZerobusSdk#streamBuilder(String)} instead of calling this constructor directly.
   */
  ZerobusStreamBuilder(
      @Nonnull ZerobusSdkStubFactory stubFactory,
      @Nonnull ExecutorService executor,
      @Nonnull String workspaceId,
      @Nonnull String serverEndpoint,
      @Nonnull String unityCatalogEndpoint,
      @Nonnull String tableName) {
    this.stubFactory = Objects.requireNonNull(stubFactory, "stubFactory cannot be null");
    this.executor = Objects.requireNonNull(executor, "executor cannot be null");
    this.workspaceId = Objects.requireNonNull(workspaceId, "workspaceId cannot be null");
    this.serverEndpoint = Objects.requireNonNull(serverEndpoint, "serverEndpoint cannot be null");
    this.unityCatalogEndpoint =
        Objects.requireNonNull(unityCatalogEndpoint, "unityCatalogEndpoint cannot be null");
    Objects.requireNonNull(tableName, "tableName cannot be null");
    if (tableName.trim().isEmpty()) {
      throw new IllegalArgumentException("tableName cannot be empty");
    }
    this.tableName = tableName;
  }

  /**
   * Sets the OAuth client credentials and returns an authenticated builder.
   *
   * <p>This is the most common authentication method. The SDK will use OAuth 2.0 with Unity Catalog
   * to authenticate requests.
   *
   * @param clientId The OAuth client ID (must not be null or empty)
   * @param clientSecret The OAuth client secret (must not be null or empty)
   * @return an authenticated builder for configuring and building the stream
   * @throws IllegalArgumentException if clientId or clientSecret is null or empty
   */
  @Nonnull
  public AuthenticatedZerobusStreamBuilder clientCredentials(
      @Nonnull String clientId, @Nonnull String clientSecret) {
    Objects.requireNonNull(clientId, "clientId cannot be null");
    Objects.requireNonNull(clientSecret, "clientSecret cannot be null");
    if (clientId.trim().isEmpty()) {
      throw new IllegalArgumentException("clientId cannot be empty");
    }
    if (clientSecret.trim().isEmpty()) {
      throw new IllegalArgumentException("clientSecret cannot be empty");
    }
    return new AuthenticatedZerobusStreamBuilder(
        stubFactory,
        executor,
        workspaceId,
        serverEndpoint,
        unityCatalogEndpoint,
        tableName,
        clientId,
        clientSecret);
  }

  /**
   * Returns an unauthenticated builder for custom authentication.
   *
   * <p>Use this when you want to provide your own authentication mechanism via {@link
   * UnauthenticatedZerobusStreamBuilder#headersProvider}. If no headers provider is set, streams
   * will be created without authentication headers.
   *
   * @return an unauthenticated builder for configuring and building the stream
   */
  @Nonnull
  public UnauthenticatedZerobusStreamBuilder unauthenticated() {
    return new UnauthenticatedZerobusStreamBuilder(
        stubFactory, executor, workspaceId, serverEndpoint, unityCatalogEndpoint, tableName);
  }

  // ==================== Base Configurable Builder ====================

  /**
   * Abstract base class for stream builders that provides common configuration methods.
   *
   * @param <SELF> The concrete builder type for method chaining
   */
  abstract static class BaseConfigurableZerobusStreamBuilder<
      SELF extends BaseConfigurableZerobusStreamBuilder<SELF>> {
    protected final ZerobusSdkStubFactory stubFactory;
    protected final ExecutorService executor;
    protected final String workspaceId;
    protected final String serverEndpoint;
    protected final String unityCatalogEndpoint;
    protected final String tableName;
    protected Optional<HeadersProvider> headersProvider = Optional.empty();
    protected Optional<TlsConfig> tlsConfig = Optional.empty();
    protected StreamConfigurationOptions.StreamConfigurationOptionsBuilder optionsBuilder =
        StreamConfigurationOptions.builder();

    BaseConfigurableZerobusStreamBuilder(
        ZerobusSdkStubFactory stubFactory,
        ExecutorService executor,
        String workspaceId,
        String serverEndpoint,
        String unityCatalogEndpoint,
        String tableName) {
      this.stubFactory = stubFactory;
      this.executor = executor;
      this.workspaceId = workspaceId;
      this.serverEndpoint = serverEndpoint;
      this.unityCatalogEndpoint = unityCatalogEndpoint;
      this.tableName = tableName;
    }

    @SuppressWarnings("unchecked")
    protected SELF self() {
      return (SELF) this;
    }

    /** Returns the client ID for OAuth, or null if not using OAuth. */
    @Nullable protected abstract String getClientId();

    /** Returns the client secret for OAuth, or null if not using OAuth. */
    @Nullable protected abstract String getClientSecret();

    /**
     * Sets a custom headers provider.
     *
     * <p>Headers providers can add custom headers to all gRPC requests. This can be used for custom
     * authentication, tracing, or other purposes.
     *
     * <p>For OAuth authentication, use {@link ZerobusStreamBuilder#clientCredentials} instead - it
     * automatically sets up an OAuth headers provider.
     *
     * @param headersProvider The custom headers provider
     * @return this builder for method chaining
     */
    @Nonnull
    public SELF headersProvider(@Nonnull HeadersProvider headersProvider) {
      this.headersProvider =
          Optional.of(Objects.requireNonNull(headersProvider, "headersProvider cannot be null"));
      return self();
    }

    /**
     * Sets a custom TLS configuration.
     *
     * <p>If not set, the default secure TLS configuration will be used.
     *
     * @param tlsConfig The TLS configuration
     * @return this builder for method chaining
     */
    @Nonnull
    public SELF tlsConfig(@Nonnull TlsConfig tlsConfig) {
      this.tlsConfig = Optional.of(Objects.requireNonNull(tlsConfig, "tlsConfig cannot be null"));
      return self();
    }

    /**
     * Sets the maximum number of requests that can be in flight.
     *
     * @param maxInflightRequests the maximum number of in-flight requests (must be positive)
     * @return this builder for method chaining
     * @throws IllegalArgumentException if maxInflightRequests is not positive
     */
    @Nonnull
    public SELF maxInflightRequests(int maxInflightRequests) {
      if (maxInflightRequests <= 0) {
        throw new IllegalArgumentException(
            "maxInflightRequests must be positive, got: " + maxInflightRequests);
      }
      this.optionsBuilder.setMaxInflightRequests(maxInflightRequests);
      return self();
    }

    /**
     * Sets whether automatic recovery is enabled.
     *
     * @param recovery true to enable automatic recovery, false to disable
     * @return this builder for method chaining
     */
    @Nonnull
    public SELF recovery(boolean recovery) {
      this.optionsBuilder.setRecovery(recovery);
      return self();
    }

    /**
     * Sets the timeout for recovery operations in milliseconds.
     *
     * @param recoveryTimeoutMs the recovery timeout (must be non-negative)
     * @return this builder for method chaining
     * @throws IllegalArgumentException if recoveryTimeoutMs is negative
     */
    @Nonnull
    public SELF recoveryTimeoutMs(int recoveryTimeoutMs) {
      if (recoveryTimeoutMs < 0) {
        throw new IllegalArgumentException(
            "recoveryTimeoutMs must be non-negative, got: " + recoveryTimeoutMs);
      }
      this.optionsBuilder.setRecoveryTimeoutMs(recoveryTimeoutMs);
      return self();
    }

    /**
     * Sets the backoff delay between recovery attempts in milliseconds.
     *
     * @param recoveryBackoffMs the backoff delay (must be non-negative)
     * @return this builder for method chaining
     * @throws IllegalArgumentException if recoveryBackoffMs is negative
     */
    @Nonnull
    public SELF recoveryBackoffMs(int recoveryBackoffMs) {
      if (recoveryBackoffMs < 0) {
        throw new IllegalArgumentException(
            "recoveryBackoffMs must be non-negative, got: " + recoveryBackoffMs);
      }
      this.optionsBuilder.setRecoveryBackoffMs(recoveryBackoffMs);
      return self();
    }

    /**
     * Sets the maximum number of recovery attempts.
     *
     * @param recoveryRetries the maximum retries (must be non-negative)
     * @return this builder for method chaining
     * @throws IllegalArgumentException if recoveryRetries is negative
     */
    @Nonnull
    public SELF recoveryRetries(int recoveryRetries) {
      if (recoveryRetries < 0) {
        throw new IllegalArgumentException(
            "recoveryRetries must be non-negative, got: " + recoveryRetries);
      }
      this.optionsBuilder.setRecoveryRetries(recoveryRetries);
      return self();
    }

    /**
     * Sets the timeout for flush operations in milliseconds.
     *
     * @param flushTimeoutMs the flush timeout (must be non-negative)
     * @return this builder for method chaining
     * @throws IllegalArgumentException if flushTimeoutMs is negative
     */
    @Nonnull
    public SELF flushTimeoutMs(int flushTimeoutMs) {
      if (flushTimeoutMs < 0) {
        throw new IllegalArgumentException(
            "flushTimeoutMs must be non-negative, got: " + flushTimeoutMs);
      }
      this.optionsBuilder.setFlushTimeoutMs(flushTimeoutMs);
      return self();
    }

    /**
     * Sets the timeout for server acknowledgment in milliseconds.
     *
     * @param serverLackOfAckTimeoutMs the acknowledgment timeout (must be positive)
     * @return this builder for method chaining
     * @throws IllegalArgumentException if serverLackOfAckTimeoutMs is not positive
     */
    @Nonnull
    public SELF serverLackOfAckTimeoutMs(int serverLackOfAckTimeoutMs) {
      if (serverLackOfAckTimeoutMs <= 0) {
        throw new IllegalArgumentException(
            "serverLackOfAckTimeoutMs must be positive, got: " + serverLackOfAckTimeoutMs);
      }
      this.optionsBuilder.setServerLackOfAckTimeoutMs(serverLackOfAckTimeoutMs);
      return self();
    }

    /**
     * Sets the maximum message size in bytes for gRPC messages.
     *
     * @param maxMessageSizeBytes the maximum message size (must be positive)
     * @return this builder for method chaining
     * @throws IllegalArgumentException if maxMessageSizeBytes is not positive
     */
    @Nonnull
    public SELF maxMessageSizeBytes(int maxMessageSizeBytes) {
      if (maxMessageSizeBytes <= 0) {
        throw new IllegalArgumentException(
            "maxMessageSizeBytes must be positive, got: " + maxMessageSizeBytes);
      }
      this.optionsBuilder.setMaxMessageSizeBytes(maxMessageSizeBytes);
      return self();
    }

    /**
     * Sets the offset acknowledgment callback function.
     *
     * @param offsetCallback the callback that receives acknowledged offset IDs
     * @return this builder for method chaining
     */
    @Nonnull
    public SELF offsetCallback(@Nullable LongConsumer offsetCallback) {
      this.optionsBuilder.setOffsetCallback(offsetCallback);
      return self();
    }

    /**
     * Sets all options from an existing StreamConfigurationOptions object.
     *
     * <p>This is useful when recreating streams with the same configuration.
     *
     * @param options The options to copy
     * @return this builder for method chaining
     */
    @Nonnull
    public SELF options(@Nonnull StreamConfigurationOptions options) {
      Objects.requireNonNull(options, "options cannot be null");
      this.optionsBuilder = options.toBuilder();
      return self();
    }

    /**
     * Configures the stream for compiled protobuf records.
     *
     * @param defaultInstance The default instance of the protobuf message type
     * @param <T> The protobuf message type
     * @return a typed builder for completing the stream creation
     */
    @Nonnull
    public <T extends Message> ProtoZerobusStreamBuilder<T> compiledProto(
        @Nonnull T defaultInstance) {
      Objects.requireNonNull(defaultInstance, "defaultInstance cannot be null");
      ProtoTableProperties tableProperties =
          ProtoTableProperties.fromCompiled(tableName, defaultInstance);
      return new ProtoZerobusStreamBuilder<>(
          stubFactory,
          executor,
          workspaceId,
          serverEndpoint,
          unityCatalogEndpoint,
          tableProperties,
          getClientId(),
          getClientSecret(),
          optionsBuilder.build(),
          headersProvider,
          tlsConfig);
    }

    /**
     * Configures the stream for dynamic protobuf records.
     *
     * @param descriptor The protobuf descriptor created or loaded at runtime
     * @return a typed builder for completing the stream creation
     */
    @Nonnull
    public ProtoZerobusStreamBuilder<DynamicMessage> dynamicProto(
        @Nonnull Descriptors.Descriptor descriptor) {
      Objects.requireNonNull(descriptor, "descriptor cannot be null");
      ProtoTableProperties tableProperties =
          ProtoTableProperties.fromDynamic(tableName, descriptor);
      return new ProtoZerobusStreamBuilder<>(
          stubFactory,
          executor,
          workspaceId,
          serverEndpoint,
          unityCatalogEndpoint,
          tableProperties,
          getClientId(),
          getClientSecret(),
          optionsBuilder.build(),
          headersProvider,
          tlsConfig);
    }

    /**
     * Configures the stream for legacy proto records (package-private, for internal SDK use).
     *
     * <p>This method is used by {@link ZerobusSdk#createStream} to maintain backwards
     * compatibility.
     *
     * @param tableProperties The legacy table properties
     * @param <T> The protobuf message type
     * @return a typed builder for completing the stream creation
     * @deprecated Since 0.2.0. Use {@link #compiledProto(Message)} instead. This method will be
     *     removed in a future release.
     */
    @Deprecated
    @Nonnull
    <T extends Message> LegacyProtoZerobusStreamBuilder<T> legacyProto(
        @Nonnull TableProperties<T> tableProperties) {
      Objects.requireNonNull(tableProperties, "tableProperties cannot be null");
      return new LegacyProtoZerobusStreamBuilder<>(
          stubFactory,
          executor,
          workspaceId,
          serverEndpoint,
          unityCatalogEndpoint,
          tableProperties,
          getClientId(),
          getClientSecret(),
          optionsBuilder.build(),
          headersProvider,
          tlsConfig);
    }

    /**
     * Configures the stream for JSON records.
     *
     * @return a typed builder for completing the stream creation
     */
    @Nonnull
    public JsonZerobusStreamBuilder json() {
      JsonTableProperties tableProperties = new JsonTableProperties(tableName);
      return new JsonZerobusStreamBuilder(
          stubFactory,
          executor,
          workspaceId,
          serverEndpoint,
          unityCatalogEndpoint,
          tableProperties,
          getClientId(),
          getClientSecret(),
          optionsBuilder.build(),
          headersProvider,
          tlsConfig);
    }
  }

  // ==================== Authenticated Stream Builder ====================

  /**
   * Builder for streams using OAuth authentication.
   *
   * <p>This builder is returned by {@link ZerobusStreamBuilder#clientCredentials} and has OAuth
   * credentials configured. You can still override the headers provider if needed.
   */
  public static class AuthenticatedZerobusStreamBuilder
      extends BaseConfigurableZerobusStreamBuilder<AuthenticatedZerobusStreamBuilder> {

    private final String clientId;
    private final String clientSecret;

    AuthenticatedZerobusStreamBuilder(
        ZerobusSdkStubFactory stubFactory,
        ExecutorService executor,
        String workspaceId,
        String serverEndpoint,
        String unityCatalogEndpoint,
        String tableName,
        String clientId,
        String clientSecret) {
      super(stubFactory, executor, workspaceId, serverEndpoint, unityCatalogEndpoint, tableName);
      this.clientId = clientId;
      this.clientSecret = clientSecret;
    }

    @Override
    protected String getClientId() {
      return clientId;
    }

    @Override
    protected String getClientSecret() {
      return clientSecret;
    }
  }

  // ==================== Unauthenticated Stream Builder ====================

  /**
   * Builder for streams without OAuth authentication.
   *
   * <p>This builder is returned by {@link ZerobusStreamBuilder#unauthenticated}. Use {@link
   * #headersProvider} to set custom authentication headers, or leave it unset for unauthenticated
   * requests.
   */
  public static class UnauthenticatedZerobusStreamBuilder
      extends BaseConfigurableZerobusStreamBuilder<UnauthenticatedZerobusStreamBuilder> {

    UnauthenticatedZerobusStreamBuilder(
        ZerobusSdkStubFactory stubFactory,
        ExecutorService executor,
        String workspaceId,
        String serverEndpoint,
        String unityCatalogEndpoint,
        String tableName) {
      super(stubFactory, executor, workspaceId, serverEndpoint, unityCatalogEndpoint, tableName);
    }

    @Override
    protected String getClientId() {
      return null;
    }

    @Override
    protected String getClientSecret() {
      return null;
    }
  }

  // ==================== Typed Stream Builders ====================

  /**
   * Builder for proto streams (final step after schema selection).
   *
   * @param <T> The protobuf message type
   */
  public static class ProtoZerobusStreamBuilder<T extends Message> {
    private static final Logger logger = LoggerFactory.getLogger(ProtoZerobusStreamBuilder.class);

    private final ZerobusSdkStubFactory stubFactory;
    private final ExecutorService executor;
    private final String workspaceId;
    private final String serverEndpoint;
    private final String unityCatalogEndpoint;
    private final ProtoTableProperties tableProperties;
    private final String clientId;
    private final String clientSecret;
    private final StreamConfigurationOptions options;
    private final Optional<HeadersProvider> headersProvider;
    private final Optional<TlsConfig> tlsConfig;

    ProtoZerobusStreamBuilder(
        ZerobusSdkStubFactory stubFactory,
        ExecutorService executor,
        String workspaceId,
        String serverEndpoint,
        String unityCatalogEndpoint,
        ProtoTableProperties tableProperties,
        String clientId,
        String clientSecret,
        StreamConfigurationOptions options,
        Optional<HeadersProvider> headersProvider,
        Optional<TlsConfig> tlsConfig) {
      this.stubFactory = stubFactory;
      this.executor = executor;
      this.workspaceId = workspaceId;
      this.serverEndpoint = serverEndpoint;
      this.unityCatalogEndpoint = unityCatalogEndpoint;
      this.tableProperties = tableProperties;
      this.clientId = clientId;
      this.clientSecret = clientSecret;
      this.options = options;
      this.headersProvider = headersProvider;
      this.tlsConfig = tlsConfig;
    }

    /**
     * Builds and initializes the proto stream.
     *
     * @return CompletableFuture that completes with the stream
     */
    @Nonnull
    public CompletableFuture<ProtoZerobusStream<T>> build() {
      CompletableFuture<ProtoZerobusStream<T>> result = new CompletableFuture<>();

      try {
        logger.debug("Creating proto stream for table: {}", tableProperties.getTableName());

        HeadersProvider effectiveHeaders =
            headersProvider.orElseGet(
                () ->
                    new OAuthHeadersProvider(
                        tableProperties.getTableName(),
                        workspaceId,
                        unityCatalogEndpoint,
                        clientId,
                        clientSecret));
        TlsConfig effectiveTls = tlsConfig.orElseGet(SecureTlsConfig::new);

        String tableName = tableProperties.getTableName();
        Supplier<ZerobusGrpc.ZerobusStub> stubSupplier =
            () ->
                stubFactory.createStub(
                    serverEndpoint,
                    effectiveHeaders,
                    effectiveTls,
                    options.maxMessageSizeBytes(),
                    tableName);

        ProtoZerobusStream<T> stream =
            new ProtoZerobusStream<>(
                stubSupplier,
                tableProperties,
                clientId,
                clientSecret,
                effectiveHeaders,
                effectiveTls,
                options,
                executor);

        stream
            .initialize()
            .whenComplete(
                (r, e) -> {
                  if (e == null) {
                    result.complete(stream);
                  } else {
                    result.completeExceptionally(e);
                  }
                });
      } catch (Throwable e) {
        logger.error("Failed to create proto stream", e);
        result.completeExceptionally(
            e instanceof ZerobusException ? e : new ZerobusException(e.getMessage(), e));
      }
      return result;
    }
  }

  /** Builder for JSON streams (final step after schema selection). */
  public static class JsonZerobusStreamBuilder {
    private static final Logger logger = LoggerFactory.getLogger(JsonZerobusStreamBuilder.class);

    private final ZerobusSdkStubFactory stubFactory;
    private final ExecutorService executor;
    private final String workspaceId;
    private final String serverEndpoint;
    private final String unityCatalogEndpoint;
    private final JsonTableProperties tableProperties;
    private final String clientId;
    private final String clientSecret;
    private final StreamConfigurationOptions options;
    private final Optional<HeadersProvider> headersProvider;
    private final Optional<TlsConfig> tlsConfig;

    JsonZerobusStreamBuilder(
        ZerobusSdkStubFactory stubFactory,
        ExecutorService executor,
        String workspaceId,
        String serverEndpoint,
        String unityCatalogEndpoint,
        JsonTableProperties tableProperties,
        String clientId,
        String clientSecret,
        StreamConfigurationOptions options,
        Optional<HeadersProvider> headersProvider,
        Optional<TlsConfig> tlsConfig) {
      this.stubFactory = stubFactory;
      this.executor = executor;
      this.workspaceId = workspaceId;
      this.serverEndpoint = serverEndpoint;
      this.unityCatalogEndpoint = unityCatalogEndpoint;
      this.tableProperties = tableProperties;
      this.clientId = clientId;
      this.clientSecret = clientSecret;
      this.options = options;
      this.headersProvider = headersProvider;
      this.tlsConfig = tlsConfig;
    }

    /**
     * Builds and initializes the JSON stream.
     *
     * @return CompletableFuture that completes with the stream
     */
    @Nonnull
    public CompletableFuture<JsonZerobusStream> build() {
      CompletableFuture<JsonZerobusStream> result = new CompletableFuture<>();

      try {
        logger.debug("Creating JSON stream for table: {}", tableProperties.getTableName());

        HeadersProvider effectiveHeaders =
            headersProvider.orElseGet(
                () ->
                    new OAuthHeadersProvider(
                        tableProperties.getTableName(),
                        workspaceId,
                        unityCatalogEndpoint,
                        clientId,
                        clientSecret));
        TlsConfig effectiveTls = tlsConfig.orElseGet(SecureTlsConfig::new);

        String tableName = tableProperties.getTableName();
        Supplier<ZerobusGrpc.ZerobusStub> stubSupplier =
            () ->
                stubFactory.createStub(
                    serverEndpoint,
                    effectiveHeaders,
                    effectiveTls,
                    options.maxMessageSizeBytes(),
                    tableName);

        JsonZerobusStream stream =
            new JsonZerobusStream(
                stubSupplier,
                tableProperties,
                clientId,
                clientSecret,
                effectiveHeaders,
                effectiveTls,
                options,
                executor);

        stream
            .initialize()
            .whenComplete(
                (r, e) -> {
                  if (e == null) {
                    result.complete(stream);
                  } else {
                    result.completeExceptionally(e);
                  }
                });
      } catch (Throwable e) {
        logger.error("Failed to create JSON stream", e);
        result.completeExceptionally(
            e instanceof ZerobusException ? e : new ZerobusException(e.getMessage(), e));
      }
      return result;
    }
  }

  /**
   * Builder for legacy proto streams (package-private, for internal SDK use).
   *
   * <p>This builder is used by {@link ZerobusSdk#createStream} to maintain backwards compatibility
   * with the deprecated {@link ZerobusStream} type.
   *
   * @param <T> The protobuf message type
   * @deprecated Since 0.2.0. Use {@link ProtoZerobusStreamBuilder} instead. This class will be
   *     removed in a future release.
   */
  @Deprecated
  static class LegacyProtoZerobusStreamBuilder<T extends Message> {
    private static final Logger logger =
        LoggerFactory.getLogger(LegacyProtoZerobusStreamBuilder.class);

    private final ZerobusSdkStubFactory stubFactory;
    private final ExecutorService executor;
    private final String workspaceId;
    private final String serverEndpoint;
    private final String unityCatalogEndpoint;
    private final TableProperties<T> tableProperties;
    private final String clientId;
    private final String clientSecret;
    private final StreamConfigurationOptions options;
    private final Optional<HeadersProvider> headersProvider;
    private final Optional<TlsConfig> tlsConfig;

    LegacyProtoZerobusStreamBuilder(
        ZerobusSdkStubFactory stubFactory,
        ExecutorService executor,
        String workspaceId,
        String serverEndpoint,
        String unityCatalogEndpoint,
        TableProperties<T> tableProperties,
        String clientId,
        String clientSecret,
        StreamConfigurationOptions options,
        Optional<HeadersProvider> headersProvider,
        Optional<TlsConfig> tlsConfig) {
      this.stubFactory = stubFactory;
      this.executor = executor;
      this.workspaceId = workspaceId;
      this.serverEndpoint = serverEndpoint;
      this.unityCatalogEndpoint = unityCatalogEndpoint;
      this.tableProperties = tableProperties;
      this.clientId = clientId;
      this.clientSecret = clientSecret;
      this.options = options;
      this.headersProvider = headersProvider;
      this.tlsConfig = tlsConfig;
    }

    /**
     * Builds and initializes the legacy proto stream.
     *
     * @return CompletableFuture that completes with the stream
     * @deprecated Since 0.2.0. Use {@link ProtoZerobusStreamBuilder#build()} instead. This method
     *     will be removed in a future release.
     */
    @Deprecated
    @Nonnull
    CompletableFuture<ZerobusStream<T>> build() {
      CompletableFuture<ZerobusStream<T>> result = new CompletableFuture<>();

      try {
        logger.debug("Creating legacy stream for table: {}", tableProperties.getTableName());

        HeadersProvider effectiveHeaders =
            headersProvider.orElseGet(
                () ->
                    new OAuthHeadersProvider(
                        tableProperties.getTableName(),
                        workspaceId,
                        unityCatalogEndpoint,
                        clientId,
                        clientSecret));
        TlsConfig effectiveTls = tlsConfig.orElseGet(SecureTlsConfig::new);

        String tableName = tableProperties.getTableName();
        Supplier<ZerobusGrpc.ZerobusStub> stubSupplier =
            () ->
                stubFactory.createStub(
                    serverEndpoint,
                    effectiveHeaders,
                    effectiveTls,
                    options.maxMessageSizeBytes(),
                    tableName);

        ZerobusStream<T> stream =
            new ZerobusStream<>(
                stubSupplier,
                tableProperties,
                clientId,
                clientSecret,
                effectiveHeaders,
                effectiveTls,
                options,
                executor);

        stream
            .initialize()
            .whenComplete(
                (r, e) -> {
                  if (e == null) {
                    result.complete(stream);
                  } else {
                    result.completeExceptionally(e);
                  }
                });
      } catch (Throwable e) {
        logger.error("Failed to create legacy stream", e);
        result.completeExceptionally(
            e instanceof ZerobusException ? e : new ZerobusException(e.getMessage(), e));
      }
      return result;
    }
  }
}
