package com.databricks.zerobus.batch.json;

import com.databricks.zerobus.batch.PrimaryBatch;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import javax.annotation.Nonnull;

/**
 * Wrapper for a batch of Map records to be serialized as JSON.
 *
 * <p>This is the primary batch type for {@link com.databricks.zerobus.stream.JsonZerobusStream},
 * containing structured Map records that will be serialized to JSON.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * List<Map<String, Object>> records = new ArrayList<>();
 * records.add(Map.of("name", "Alice", "age", 30));
 * records.add(Map.of("name", "Bob", "age", 25));
 * Long offset = stream.ingestBatch(MapBatch.of(records));
 * }</pre>
 *
 * @see com.databricks.zerobus.stream.JsonZerobusStream#ingestBatch(PrimaryBatch)
 */
public final class MapBatch implements PrimaryBatch<Map<String, ?>> {

  private final Iterable<Map<String, ?>> maps;

  private MapBatch(Iterable<Map<String, ?>> maps) {
    this.maps = maps;
  }

  /**
   * Creates a MapBatch from an iterable of Maps.
   *
   * @param maps The Maps to wrap
   * @return A new MapBatch instance
   */
  @Nonnull
  public static MapBatch of(@Nonnull Iterable<Map<String, ?>> maps) {
    if (maps == null) {
      throw new IllegalArgumentException("maps cannot be null");
    }
    return new MapBatch(maps);
  }

  /**
   * Creates a MapBatch from varargs Maps.
   *
   * @param maps The Maps to wrap
   * @return A new MapBatch instance
   */
  @SafeVarargs
  @Nonnull
  public static MapBatch of(@Nonnull Map<String, ?>... maps) {
    if (maps == null) {
      throw new IllegalArgumentException("maps cannot be null");
    }
    return new MapBatch(Arrays.asList(maps));
  }

  @Override
  @Nonnull
  public Iterator<Map<String, ?>> iterator() {
    return maps.iterator();
  }
}
