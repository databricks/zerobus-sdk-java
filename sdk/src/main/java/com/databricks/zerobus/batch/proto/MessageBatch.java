package com.databricks.zerobus.batch.proto;

import com.databricks.zerobus.batch.PrimaryBatch;
import com.google.protobuf.Message;
import java.util.Arrays;
import java.util.Iterator;
import javax.annotation.Nonnull;

/**
 * Wrapper for a batch of protobuf message records.
 *
 * <p>This is the primary batch type for {@link com.databricks.zerobus.stream.ProtoZerobusStream},
 * containing structured protobuf messages.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * List<MyRecord> records = new ArrayList<>();
 * records.add(MyRecord.newBuilder().setField("value1").build());
 * records.add(MyRecord.newBuilder().setField("value2").build());
 * Long offset = stream.ingestBatch(MessageBatch.of(records));
 * }</pre>
 *
 * @param <T> The protobuf message type
 * @see com.databricks.zerobus.stream.ProtoZerobusStream#ingestBatch(PrimaryBatch)
 */
public final class MessageBatch<T extends Message> implements PrimaryBatch<T> {

  private final Iterable<T> records;

  private MessageBatch(Iterable<T> records) {
    this.records = records;
  }

  /**
   * Creates a MessageBatch from an iterable of protobuf messages.
   *
   * @param records The protobuf messages to wrap
   * @param <T> The protobuf message type
   * @return A new MessageBatch instance
   */
  @Nonnull
  public static <T extends Message> MessageBatch<T> of(@Nonnull Iterable<T> records) {
    if (records == null) {
      throw new IllegalArgumentException("records cannot be null");
    }
    return new MessageBatch<>(records);
  }

  /**
   * Creates a MessageBatch from varargs protobuf messages.
   *
   * @param records The protobuf messages to wrap
   * @param <T> The protobuf message type
   * @return A new MessageBatch instance
   */
  @SafeVarargs
  @Nonnull
  public static <T extends Message> MessageBatch<T> of(@Nonnull T... records) {
    if (records == null) {
      throw new IllegalArgumentException("records cannot be null");
    }
    return new MessageBatch<>(Arrays.asList(records));
  }

  @Override
  @Nonnull
  public Iterator<T> iterator() {
    return records.iterator();
  }
}
