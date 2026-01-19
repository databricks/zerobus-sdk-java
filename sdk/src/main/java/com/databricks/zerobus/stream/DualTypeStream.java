package com.databricks.zerobus.stream;

import com.databricks.zerobus.StreamConfigurationOptions;
import com.databricks.zerobus.ZerobusGrpc.ZerobusStub;
import com.databricks.zerobus.ZerobusSdk;
import com.databricks.zerobus.auth.HeadersProvider;
import com.databricks.zerobus.schema.BaseTableProperties;
import com.databricks.zerobus.tls.TlsConfig;
import com.google.protobuf.DescriptorProtos;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;
import javax.annotation.Nonnull;

/**
 * Abstract base class for Zerobus streams with both primary and secondary record types.
 *
 * <p>This class extends {@link BaseZerobusStream} to add support for a secondary record type. The
 * primary type is typically the "structured" or "native" type, while the secondary type is the
 * "raw" or "serialized" type.
 *
 * <p>For protobuf streams:
 *
 * <ul>
 *   <li>Primary: {@code T extends Message} - structured protobuf messages
 *   <li>Secondary: {@code byte[]} - pre-serialized bytes
 * </ul>
 *
 * <p>For JSON streams:
 *
 * <ul>
 *   <li>Primary: {@code Map<String, ?>} - structured Map records
 *   <li>Secondary: {@code String} - raw JSON strings
 * </ul>
 *
 * @param <P> The primary record type (structured)
 * @param <S> The secondary record type (raw/serialized)
 * @see BaseZerobusStream
 * @see ProtoZerobusStream
 * @see JsonZerobusStream
 */
public abstract class DualTypeStream<P, S> extends BaseZerobusStream<P> {

  // ==================== Constructor ====================

  protected DualTypeStream(
      Supplier<ZerobusStub> stubSupplier,
      BaseTableProperties tableProperties,
      String clientId,
      String clientSecret,
      HeadersProvider headersProvider,
      TlsConfig tlsConfig,
      StreamConfigurationOptions options,
      ExecutorService executor,
      DescriptorProtos.DescriptorProto descriptorProto) {
    super(
        stubSupplier,
        tableProperties,
        clientId,
        clientSecret,
        headersProvider,
        tlsConfig,
        options,
        executor,
        descriptorProto);
  }

  // ==================== Secondary Type Methods ====================
  //
  // Note: Secondary type methods (ingest(S) and ingestBatch(SecondaryBatch<S>)) are NOT declared
  // as abstract here due to Java type erasure. Abstract methods ingest(P) and ingest(S) would
  // have the same erasure (ingest(Object)) and conflict.
  //
  // Concrete subclasses MUST provide:
  //   - public long ingest(S record) throws ZerobusException
  //   - public Long ingestBatch(SecondaryBatch<S> batch) throws ZerobusException
  //
  // This works in concrete classes because their type parameters have different erasures:
  //   - ProtoZerobusStream<T>: ingest(T) erases to ingest(Message), ingest(byte[]) is concrete
  //   - JsonZerobusStream: ingest(Map) and ingest(String) are both concrete

  // ==================== Override recreate to return DualTypeStream ====================

  /**
   * Recreates this stream with the same configuration.
   *
   * <p>Creates a new stream and re-ingests any unacknowledged records from this stream.
   *
   * @param sdk The SDK instance to use for recreation
   * @return A future that completes with the new stream
   */
  @Override
  @Nonnull
  public abstract CompletableFuture<? extends DualTypeStream<P, S>> recreate(ZerobusSdk sdk);
}
