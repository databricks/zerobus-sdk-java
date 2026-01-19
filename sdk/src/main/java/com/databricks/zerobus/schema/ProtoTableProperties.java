package com.databricks.zerobus.schema;

import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;

/**
 * Table properties for protocol buffer record ingestion.
 *
 * <p>This class stores the table name and the protobuf descriptor proto which defines the schema
 * for records being ingested. Use the factory methods to create instances:
 *
 * <ul>
 *   <li>{@link #fromCompiled(String, Message)} - for compiled .proto schemas
 *   <li>{@link #fromDynamic(String, Descriptor)} - for runtime-created schemas
 * </ul>
 *
 * <p>Example usage with compiled schema:
 *
 * <pre>{@code
 * ProtoZerobusStream<MyRecord> stream = sdk.streamBuilder("catalog.schema.table")
 *     .clientCredentials(clientId, clientSecret)
 *     .compiledProto(MyRecord.getDefaultInstance())
 *     .build()
 *     .join();
 * }</pre>
 *
 * <p>Example usage with dynamic schema:
 *
 * <pre>{@code
 * ProtoZerobusStream<DynamicMessage> stream = sdk.streamBuilder("catalog.schema.table")
 *     .clientCredentials(clientId, clientSecret)
 *     .dynamicProto(descriptor)
 *     .build()
 *     .join();
 * }</pre>
 *
 * @see JsonTableProperties
 * @see com.databricks.zerobus.stream.ProtoZerobusStream
 */
public class ProtoTableProperties extends BaseTableProperties {

  private final DescriptorProto descriptorProto;
  private final Message defaultInstance;

  /**
   * Creates table properties with a pre-computed descriptor proto.
   *
   * <p>Prefer using the factory methods {@link #fromCompiled(String, Message)} or {@link
   * #fromDynamic(String, Descriptor)} instead of this constructor.
   *
   * @param tableName the fully qualified table name (catalog.schema.table)
   * @param descriptorProto the protobuf descriptor proto for the schema
   */
  public ProtoTableProperties(String tableName, DescriptorProto descriptorProto) {
    this(tableName, descriptorProto, null);
  }

  /**
   * Creates table properties with a descriptor proto and optional default instance.
   *
   * @param tableName the fully qualified table name (catalog.schema.table)
   * @param descriptorProto the protobuf descriptor proto for the schema
   * @param defaultInstance the default instance (for compiled schemas, null for dynamic)
   */
  private ProtoTableProperties(
      String tableName, DescriptorProto descriptorProto, Message defaultInstance) {
    super(tableName);
    this.descriptorProto = descriptorProto;
    this.defaultInstance = defaultInstance;
  }

  /**
   * Creates table properties from a compiled protobuf message type.
   *
   * <p>Use this factory method when you have a compiled .proto file and generated Java classes. The
   * schema is derived from the default instance of your protobuf message type.
   *
   * @param tableName the fully qualified table name (catalog.schema.table)
   * @param defaultInstance the default instance of the protobuf message type
   * @param <T> the protobuf message type
   * @return table properties for the compiled schema
   */
  public static <T extends Message> ProtoTableProperties fromCompiled(
      String tableName, T defaultInstance) {
    return new ProtoTableProperties(
        tableName, defaultInstance.getDescriptorForType().toProto(), defaultInstance);
  }

  /**
   * Creates table properties from a dynamic protobuf descriptor.
   *
   * <p>Use this factory method when you need to define a protobuf schema at runtime without a
   * compiled .proto file.
   *
   * @param tableName the fully qualified table name (catalog.schema.table)
   * @param descriptor the protobuf descriptor created or loaded at runtime
   * @return table properties for the dynamic schema
   */
  public static ProtoTableProperties fromDynamic(String tableName, Descriptor descriptor) {
    return new ProtoTableProperties(tableName, descriptor.toProto());
  }

  /**
   * Returns the protobuf descriptor proto for the record schema.
   *
   * <p>This descriptor is used for schema validation when creating streams.
   *
   * @return the protobuf descriptor proto
   */
  public DescriptorProto getDescriptorProto() {
    return descriptorProto;
  }

  /**
   * Returns the default instance used to create this table properties.
   *
   * <p>This is only available for compiled protobuf schemas (created via {@link
   * #fromCompiled(String, Message)}). Returns null for dynamic schemas.
   *
   * @return the default instance, or null if not available (dynamic schema)
   */
  public Message getDefaultInstance() {
    return defaultInstance;
  }
}
