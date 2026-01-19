package com.databricks.zerobus.batch;

/**
 * Marker interface for primary batch types.
 *
 * <p>Primary batches contain the "structured" or "native" record type for a stream:
 *
 * <ul>
 *   <li>{@link com.databricks.zerobus.batch.proto.MessageBatch} - protobuf messages for {@link
 *       com.databricks.zerobus.stream.ProtoZerobusStream}
 *   <li>{@link com.databricks.zerobus.batch.json.MapBatch} - Map records for {@link
 *       com.databricks.zerobus.stream.JsonZerobusStream}
 * </ul>
 *
 * @param <P> The primary record type
 * @see SecondaryBatch
 */
public interface PrimaryBatch<P> extends Batch<P> {}
