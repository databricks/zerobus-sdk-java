package com.databricks.zerobus;

import com.databricks.zerobus.schema.ProtoTableProperties;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

/**
 * Table properties for the stream, describes the table to ingest records into.
 *
 * @param <RecordType> The type of records to be ingested (must extend Message).
 * @deprecated Since 0.2.0. Use {@link ProtoTableProperties} or the new builder API via {@link
 *     ZerobusSdk#streamBuilder(String)} instead. This class will be removed in a future release.
 */
@Deprecated
public class TableProperties<RecordType extends Message> extends ProtoTableProperties {

  private final RecordType defaultInstance;

  /**
   * Creates a new TableProperties instance.
   *
   * @param tableName The name of the table to ingest records into.
   * @param defaultInstance The default instance of the record type (used to get the descriptor).
   * @deprecated Since 0.2.0. Use {@link ProtoTableProperties#fromCompiled(String, Message)} or the
   *     new builder API instead.
   */
  @Deprecated
  public TableProperties(String tableName, RecordType defaultInstance) {
    super(tableName, defaultInstance.getDescriptorForType().toProto());
    this.defaultInstance = defaultInstance;
  }

  /**
   * Gets the default instance of the record type.
   *
   * @return the default instance
   * @deprecated Since 0.2.0. This method is no longer needed with the new API.
   */
  @Deprecated
  public RecordType getDefaultInstance() {
    return defaultInstance;
  }

  /**
   * Gets the descriptor for the record type.
   *
   * @return the descriptor
   * @deprecated Since 0.2.0. Use {@link ProtoTableProperties#getDescriptorProto()} instead.
   */
  @Deprecated
  Descriptors.Descriptor getDescriptor() {
    return defaultInstance.getDescriptorForType();
  }
}
