package com.databricks.zerobus;

import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.LongConsumer;
import javax.annotation.Nonnull;

/**
 * Configuration options for Zerobus streams.
 *
 * <p>This class provides various settings to control stream behavior including performance tuning,
 * error handling, and callback configuration.
 *
 * <p>Use the builder pattern to create instances:
 *
 * <p>StreamConfigurationOptions options = StreamConfigurationOptions.builder()
 * .setMaxInflightRecords(50000) .setRecovery(true) .setAckCallback(response ->
 * System.out.println("Acked: " + response.getDurabilityAckUpToOffset())) .build();
 */
public class StreamConfigurationOptions {

  /** Default max message size: 10MB (matches server limit). */
  public static final int DEFAULT_MAX_MESSAGE_SIZE_BYTES = 10 * 1024 * 1024;

  private int maxInflightRequests = 50000;
  private boolean recovery = true;
  private int recoveryTimeoutMs = 15000;
  private int recoveryBackoffMs = 2000;
  private int recoveryRetries = 3;
  private int flushTimeoutMs = 300000;
  private int serverLackOfAckTimeoutMs = 60000;
  private int maxMessageSizeBytes = DEFAULT_MAX_MESSAGE_SIZE_BYTES;
  private Optional<Long> streamPausedMaxWaitTimeMs = Optional.empty();
  private Optional<LongConsumer> offsetCallback = Optional.empty();

  private StreamConfigurationOptions() {}

  private StreamConfigurationOptions(
      int maxInflightRequests,
      boolean recovery,
      int recoveryTimeoutMs,
      int recoveryBackoffMs,
      int recoveryRetries,
      int flushTimeoutMs,
      int serverLackOfAckTimeoutMs,
      int maxMessageSizeBytes,
      Optional<Long> streamPausedMaxWaitTimeMs,
      Optional<LongConsumer> offsetCallback) {
    this.maxInflightRequests = maxInflightRequests;
    this.recovery = recovery;
    this.recoveryTimeoutMs = recoveryTimeoutMs;
    this.recoveryBackoffMs = recoveryBackoffMs;
    this.recoveryRetries = recoveryRetries;
    this.flushTimeoutMs = flushTimeoutMs;
    this.serverLackOfAckTimeoutMs = serverLackOfAckTimeoutMs;
    this.maxMessageSizeBytes = maxMessageSizeBytes;
    this.streamPausedMaxWaitTimeMs = streamPausedMaxWaitTimeMs;
    this.offsetCallback = offsetCallback;
  }

  /**
   * Returns the maximum number of requests that can be in flight.
   *
   * <p>This controls how many records the SDK can accept and send to the server before waiting for
   * acknowledgments. Higher values improve throughput but use more memory.
   *
   * @return the maximum number of in-flight requests
   */
  public int maxInflightRequests() {
    return this.maxInflightRequests;
  }

  /**
   * Returns the maximum number of records that can be in flight.
   *
   * @return the maximum number of in-flight records
   * @deprecated Since 0.2.0. Use {@link #maxInflightRequests()} instead. This method will be
   *     removed in a future release.
   */
  @Deprecated
  public int maxInflightRecords() {
    return this.maxInflightRequests;
  }

  /**
   * Returns whether automatic recovery is enabled.
   *
   * <p>When enabled, the SDK will automatically attempt to recover from stream failures by
   * recreating the stream and resending unacknowledged records.
   *
   * @return true if automatic recovery is enabled, false otherwise
   */
  public boolean recovery() {
    return this.recovery;
  }

  /**
   * Returns the timeout for recovery operations.
   *
   * <p>This is the maximum time to wait for a recovery operation to complete before considering it
   * failed.
   *
   * @return the recovery timeout in milliseconds
   */
  public int recoveryTimeoutMs() {
    return this.recoveryTimeoutMs;
  }

  /**
   * Returns the backoff delay between recovery attempts.
   *
   * <p>This is the delay to wait between consecutive recovery attempts when automatic recovery is
   * enabled.
   *
   * @return the recovery backoff delay in milliseconds
   */
  public int recoveryBackoffMs() {
    return this.recoveryBackoffMs;
  }

  /**
   * Returns the maximum number of recovery attempts.
   *
   * <p>This is the maximum number of times the SDK will attempt to recover from a stream failure
   * before giving up.
   *
   * @return the maximum number of recovery attempts
   */
  public int recoveryRetries() {
    return this.recoveryRetries;
  }

  /**
   * Returns the timeout for flush operations.
   *
   * <p>This is the maximum time to wait for a flush operation to complete before considering it
   * failed.
   *
   * @return the flush timeout in milliseconds
   */
  public int flushTimeoutMs() {
    return this.flushTimeoutMs;
  }

  /**
   * Returns the timeout for server acknowledgment.
   *
   * <p>This is the maximum time to wait for the server to acknowledge records before considering
   * the server unresponsive.
   *
   * @return the server acknowledgment timeout in milliseconds
   */
  public int serverLackOfAckTimeoutMs() {
    return this.serverLackOfAckTimeoutMs;
  }

  /**
   * Returns the maximum message size in bytes for gRPC messages.
   *
   * <p>This limits the size of individual gRPC messages (records). The server enforces a 10MB
   * limit; messages exceeding this will be rejected by the server.
   *
   * <p>Default is {@link #DEFAULT_MAX_MESSAGE_SIZE_BYTES} (10MB).
   *
   * @return the maximum message size in bytes
   */
  public int maxMessageSizeBytes() {
    return this.maxMessageSizeBytes;
  }

  /**
   * Returns the maximum time to wait in PAUSED state during graceful close.
   *
   * <p>When the server sends a close signal, the stream enters PAUSED state and waits for pending
   * acknowledgments. This setting controls the maximum time to wait before triggering recovery. If
   * all acknowledgments are received before this timeout, recovery is triggered immediately.
   *
   * <p>If not set, the stream will use the default behavior of waiting for the recovery timeout.
   *
   * @return the maximum wait time in milliseconds, or empty if not configured
   */
  public Optional<Long> streamPausedMaxWaitTimeMs() {
    return this.streamPausedMaxWaitTimeMs;
  }

  /**
   * Returns the offset acknowledgment callback function.
   *
   * <p>This callback is invoked whenever the server acknowledges records. The callback receives the
   * durability acknowledgment offset ID (the offset up to which records have been durably written).
   * If no callback is set, this returns an empty Optional.
   *
   * @return the offset callback, or an empty Optional if none is set
   */
  public Optional<LongConsumer> offsetCallback() {
    return this.offsetCallback;
  }

  /**
   * Returns the default stream configuration options.
   *
   * <p>Default values: - maxInflightRecords: 50000 - recovery: true - recoveryTimeoutMs: 15000 -
   * recoveryBackoffMs: 2000 - recoveryRetries: 3 - flushTimeoutMs: 300000 -
   * serverLackOfAckTimeoutMs: 60000 - maxMessageSizeBytes: 10MB (matches server limit) -
   * ackCallback: empty
   *
   * @return the default stream configuration options
   */
  public static StreamConfigurationOptions getDefault() {
    return new StreamConfigurationOptions();
  }

  /**
   * Returns a new builder for creating StreamConfigurationOptions.
   *
   * @return a new StreamConfigurationOptionsBuilder
   */
  public static StreamConfigurationOptionsBuilder builder() {
    return new StreamConfigurationOptionsBuilder();
  }

  /**
   * Returns a builder initialized with this instance's values.
   *
   * <p>Useful for creating a modified copy of an existing configuration.
   *
   * @return a new builder pre-populated with this instance's values
   */
  public StreamConfigurationOptionsBuilder toBuilder() {
    StreamConfigurationOptionsBuilder builder =
        new StreamConfigurationOptionsBuilder()
            .setMaxInflightRequests(this.maxInflightRequests)
            .setRecovery(this.recovery)
            .setRecoveryTimeoutMs(this.recoveryTimeoutMs)
            .setRecoveryBackoffMs(this.recoveryBackoffMs)
            .setRecoveryRetries(this.recoveryRetries)
            .setFlushTimeoutMs(this.flushTimeoutMs)
            .setServerLackOfAckTimeoutMs(this.serverLackOfAckTimeoutMs)
            .setMaxMessageSizeBytes(this.maxMessageSizeBytes);
    this.streamPausedMaxWaitTimeMs.ifPresent(builder::setStreamPausedMaxWaitTimeMs);
    this.offsetCallback.ifPresent(builder::setOffsetCallback);
    return builder;
  }

  /**
   * Builder for creating StreamConfigurationOptions instances.
   *
   * <p>This builder provides a fluent API for configuring stream options. All parameters have
   * sensible defaults if not specified.
   *
   * <p>Example usage:
   *
   * <p>StreamConfigurationOptions options = StreamConfigurationOptions.builder()
   * .setMaxInflightRecords(100000) .setRecovery(false) .setAckCallback(response ->
   * System.out.println("Record acked: " + response.getDurabilityAckUpToOffset())) .build();
   *
   * @see StreamConfigurationOptions
   * @since 1.0.0
   */
  public static class StreamConfigurationOptionsBuilder {
    private StreamConfigurationOptions defaultOptions = StreamConfigurationOptions.getDefault();

    private int maxInflightRequests = defaultOptions.maxInflightRequests();
    private boolean recovery = defaultOptions.recovery();
    private int recoveryTimeoutMs = defaultOptions.recoveryTimeoutMs();
    private int recoveryBackoffMs = defaultOptions.recoveryBackoffMs();
    private int recoveryRetries = defaultOptions.recoveryRetries();
    private int flushTimeoutMs = defaultOptions.flushTimeoutMs();
    private int serverLackOfAckTimeoutMs = defaultOptions.serverLackOfAckTimeoutMs();
    private int maxMessageSizeBytes = defaultOptions.maxMessageSizeBytes();
    private Optional<Long> streamPausedMaxWaitTimeMs = defaultOptions.streamPausedMaxWaitTimeMs();
    private Optional<LongConsumer> offsetCallback = defaultOptions.offsetCallback();

    private StreamConfigurationOptionsBuilder() {}

    /**
     * Sets the maximum number of requests that can be in flight.
     *
     * <p>This controls how many records the SDK can accept and send to the server before waiting
     * for acknowledgments. Higher values improve throughput but use more memory.
     *
     * @param maxInflightRequests the maximum number of in-flight requests
     * @return this builder for method chaining
     */
    public StreamConfigurationOptionsBuilder setMaxInflightRequests(int maxInflightRequests) {
      this.maxInflightRequests = maxInflightRequests;
      return this;
    }

    /**
     * Sets the maximum number of records that can be in flight.
     *
     * @param maxInflightRecords the maximum number of in-flight records
     * @return this builder for method chaining
     * @deprecated Since 0.2.0. Use {@link #setMaxInflightRequests(int)} instead. This method will
     *     be removed in a future release.
     */
    @Deprecated
    public StreamConfigurationOptionsBuilder setMaxInflightRecords(int maxInflightRecords) {
      return setMaxInflightRequests(maxInflightRecords);
    }

    /**
     * Sets whether automatic recovery is enabled.
     *
     * <p>When enabled, the SDK will automatically attempt to recover from stream failures by
     * recreating the stream and resending unacknowledged records.
     *
     * @param recovery true to enable automatic recovery, false to disable
     * @return this builder for method chaining
     */
    public StreamConfigurationOptionsBuilder setRecovery(boolean recovery) {
      this.recovery = recovery;
      return this;
    }

    /**
     * Sets the timeout for recovery operations.
     *
     * <p>This is the maximum time to wait for a recovery operation to complete before considering
     * it failed.
     *
     * @param recoveryTimeoutMs the recovery timeout in milliseconds
     * @return this builder for method chaining
     */
    public StreamConfigurationOptionsBuilder setRecoveryTimeoutMs(int recoveryTimeoutMs) {
      this.recoveryTimeoutMs = recoveryTimeoutMs;
      return this;
    }

    /**
     * Sets the backoff delay between recovery attempts.
     *
     * <p>This is the delay to wait between consecutive recovery attempts when automatic recovery is
     * enabled.
     *
     * @param recoveryBackoffMs the recovery backoff delay in milliseconds
     * @return this builder for method chaining
     * @throws IllegalArgumentException if recoveryBackoffMs is less than 0
     */
    public StreamConfigurationOptionsBuilder setRecoveryBackoffMs(int recoveryBackoffMs) {
      this.recoveryBackoffMs = recoveryBackoffMs;
      return this;
    }

    /**
     * Sets the maximum number of recovery attempts.
     *
     * <p>This is the maximum number of times the SDK will attempt to recover from a stream failure
     * before giving up.
     *
     * @param recoveryRetries the maximum number of recovery attempts
     * @return this builder for method chaining
     */
    public StreamConfigurationOptionsBuilder setRecoveryRetries(int recoveryRetries) {
      this.recoveryRetries = recoveryRetries;
      return this;
    }

    /**
     * Sets the timeout for flush operations.
     *
     * <p>This is the maximum time to wait for a flush operation to complete before considering it
     * failed.
     *
     * @param flushTimeoutMs the flush timeout in milliseconds
     * @return this builder for method chaining
     */
    public StreamConfigurationOptionsBuilder setFlushTimeoutMs(int flushTimeoutMs) {
      this.flushTimeoutMs = flushTimeoutMs;
      return this;
    }

    /**
     * Sets the timeout for server acknowledgment.
     *
     * <p>This is the maximum time to wait for the server to acknowledge records before considering
     * the server unresponsive.
     *
     * @param serverLackOfAckTimeoutMs the server acknowledgment timeout in milliseconds
     * @return this builder for method chaining
     */
    public StreamConfigurationOptionsBuilder setServerLackOfAckTimeoutMs(
        int serverLackOfAckTimeoutMs) {
      this.serverLackOfAckTimeoutMs = serverLackOfAckTimeoutMs;
      return this;
    }

    /**
     * Sets the maximum message size in bytes for gRPC messages.
     *
     * <p>This limits the size of individual gRPC messages (records). The server enforces a 10MB
     * limit; messages exceeding this will be rejected by the server.
     *
     * <p>Default is 10MB ({@link StreamConfigurationOptions#DEFAULT_MAX_MESSAGE_SIZE_BYTES}).
     *
     * @param maxMessageSizeBytes the maximum message size in bytes
     * @return this builder for method chaining
     */
    public StreamConfigurationOptionsBuilder setMaxMessageSizeBytes(int maxMessageSizeBytes) {
      this.maxMessageSizeBytes = maxMessageSizeBytes;
      return this;
    }

    /**
     * Sets the maximum time to wait in PAUSED state during graceful close.
     *
     * <p>When the server sends a close signal, the stream enters PAUSED state and waits for pending
     * acknowledgments. This setting controls the maximum time to wait before triggering recovery.
     * If all acknowledgments are received before this timeout, recovery is triggered immediately.
     *
     * @param streamPausedMaxWaitTimeMs the maximum wait time in milliseconds
     * @return this builder for method chaining
     */
    public StreamConfigurationOptionsBuilder setStreamPausedMaxWaitTimeMs(
        long streamPausedMaxWaitTimeMs) {
      this.streamPausedMaxWaitTimeMs = Optional.of(streamPausedMaxWaitTimeMs);
      return this;
    }

    /**
     * Sets the offset acknowledgment callback function.
     *
     * <p>This callback is invoked whenever the server acknowledges records. The callback receives
     * the durability acknowledgment offset ID (the offset up to which records have been durably
     * written).
     *
     * @param offsetCallback the offset callback function that receives the offset ID
     * @return this builder for method chaining
     */
    public StreamConfigurationOptionsBuilder setOffsetCallback(
        @Nonnull LongConsumer offsetCallback) {
      this.offsetCallback = Optional.of(offsetCallback);
      return this;
    }

    /**
     * Sets the acknowledgment callback function.
     *
     * <p>This callback receives the full {@link IngestRecordResponse} object when records are
     * acknowledged. Internally, this wraps the callback to work with the offset-based system.
     *
     * @param ackCallback the acknowledgment callback function that receives the full response
     * @return this builder for method chaining
     * @deprecated Since 0.2.0. Use {@link #setOffsetCallback(LongConsumer)} instead which provides
     *     just the offset ID. This method will be removed in a future release.
     */
    @Deprecated
    public StreamConfigurationOptionsBuilder setAckCallback(
        @Nonnull Consumer<IngestRecordResponse> ackCallback) {
      this.offsetCallback =
          Optional.of(
              offset ->
                  ackCallback.accept(
                      IngestRecordResponse.newBuilder()
                          .setDurabilityAckUpToOffset(offset)
                          .build()));
      return this;
    }

    /**
     * Builds a new StreamConfigurationOptions instance.
     *
     * @return a new StreamConfigurationOptions with the configured settings
     */
    public StreamConfigurationOptions build() {
      return new StreamConfigurationOptions(
          this.maxInflightRequests,
          this.recovery,
          this.recoveryTimeoutMs,
          this.recoveryBackoffMs,
          this.recoveryRetries,
          this.flushTimeoutMs,
          this.serverLackOfAckTimeoutMs,
          this.maxMessageSizeBytes,
          this.streamPausedMaxWaitTimeMs,
          this.offsetCallback);
    }
  }
}
