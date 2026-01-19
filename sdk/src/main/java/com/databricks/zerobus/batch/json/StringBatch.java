package com.databricks.zerobus.batch.json;

import com.databricks.zerobus.batch.SecondaryBatch;
import java.util.Arrays;
import java.util.Iterator;
import javax.annotation.Nonnull;

/**
 * Wrapper for a batch of JSON string records.
 *
 * <p>This is the secondary batch type for {@link com.databricks.zerobus.stream.JsonZerobusStream},
 * containing raw JSON strings.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * List<String> records = new ArrayList<>();
 * records.add("{\"name\": \"Alice\", \"age\": 30}");
 * records.add("{\"name\": \"Bob\", \"age\": 25}");
 * Long offset = stream.ingestBatch(StringBatch.of(records));
 * }</pre>
 *
 * @see com.databricks.zerobus.stream.JsonZerobusStream#ingestBatch(StringBatch)
 */
public final class StringBatch implements SecondaryBatch<String> {

  private final Iterable<String> records;

  private StringBatch(Iterable<String> records) {
    this.records = records;
  }

  /**
   * Creates a StringBatch from an iterable of strings.
   *
   * @param records The strings to wrap
   * @return A new StringBatch instance
   */
  @Nonnull
  public static StringBatch of(@Nonnull Iterable<String> records) {
    if (records == null) {
      throw new IllegalArgumentException("records cannot be null");
    }
    return new StringBatch(records);
  }

  /**
   * Creates a StringBatch from varargs strings.
   *
   * @param records The strings to wrap
   * @return A new StringBatch instance
   */
  @Nonnull
  public static StringBatch of(@Nonnull String... records) {
    if (records == null) {
      throw new IllegalArgumentException("records cannot be null");
    }
    return new StringBatch(Arrays.asList(records));
  }

  @Override
  @Nonnull
  public Iterator<String> iterator() {
    return records.iterator();
  }
}
