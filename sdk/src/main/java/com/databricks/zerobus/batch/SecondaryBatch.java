package com.databricks.zerobus.batch;

/**
 * Marker interface for secondary batch types.
 *
 * <p>Secondary batches contain the "raw" or "serialized" record type for a stream:
 *
 * <ul>
 *   <li>{@link com.databricks.zerobus.batch.proto.BytesBatch} - pre-serialized bytes for {@link
 *       com.databricks.zerobus.stream.ProtoZerobusStream}
 *   <li>{@link com.databricks.zerobus.batch.json.StringBatch} - JSON strings for {@link
 *       com.databricks.zerobus.stream.JsonZerobusStream}
 * </ul>
 *
 * @param <S> The secondary record type
 * @see PrimaryBatch
 */
public interface SecondaryBatch<S> extends Batch<S> {}
