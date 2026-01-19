package com.databricks.zerobus.batch.proto;

import com.databricks.zerobus.batch.SecondaryBatch;
import java.util.Arrays;
import java.util.Iterator;
import javax.annotation.Nonnull;

/**
 * Wrapper for a batch of pre-serialized protobuf byte arrays.
 *
 * <p>This is the secondary batch type for {@link com.databricks.zerobus.stream.ProtoZerobusStream},
 * containing raw serialized bytes.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * List<byte[]> serializedRecords = getSerializedRecordsFromKafka();
 * Long offset = stream.ingestBatch(BytesBatch.of(serializedRecords));
 * }</pre>
 *
 * @see com.databricks.zerobus.stream.ProtoZerobusStream#ingestBatch(BytesBatch)
 */
public final class BytesBatch implements SecondaryBatch<byte[]> {

  private final Iterable<byte[]> bytes;

  private BytesBatch(Iterable<byte[]> bytes) {
    this.bytes = bytes;
  }

  /**
   * Creates a BytesBatch from an iterable of byte arrays.
   *
   * @param bytes The byte arrays to wrap
   * @return A new BytesBatch instance
   */
  @Nonnull
  public static BytesBatch of(@Nonnull Iterable<byte[]> bytes) {
    if (bytes == null) {
      throw new IllegalArgumentException("bytes cannot be null");
    }
    return new BytesBatch(bytes);
  }

  /**
   * Creates a BytesBatch from varargs byte arrays.
   *
   * @param bytes The byte arrays to wrap
   * @return A new BytesBatch instance
   */
  @Nonnull
  public static BytesBatch of(@Nonnull byte[]... bytes) {
    if (bytes == null) {
      throw new IllegalArgumentException("bytes cannot be null");
    }
    return new BytesBatch(Arrays.asList(bytes));
  }

  @Override
  @Nonnull
  public Iterator<byte[]> iterator() {
    return bytes.iterator();
  }
}
