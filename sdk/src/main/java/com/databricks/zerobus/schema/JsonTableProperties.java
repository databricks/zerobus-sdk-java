package com.databricks.zerobus.schema;

/**
 * Table properties for JSON record ingestion.
 *
 * <p>Use this class when ingesting JSON records without a protobuf schema. JSON records are
 * provided as strings or Maps.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * // Create a JSON stream using the builder API.
 * JsonZerobusStream stream = sdk.streamBuilder("catalog.schema.table")
 *     .clientCredentials(clientId, clientSecret)
 *     .json()
 *     .build()
 *     .join();
 *
 * // Ingest JSON string.
 * stream.ingest("{\"city\": \"Tokyo\", \"population\": 14000000}");
 * stream.close();
 * }</pre>
 *
 * @see ProtoTableProperties
 * @see com.databricks.zerobus.stream.JsonZerobusStream
 */
public class JsonTableProperties extends BaseTableProperties {

  /**
   * Creates table properties for JSON ingestion.
   *
   * @param tableName the fully qualified table name (catalog.schema.table)
   */
  public JsonTableProperties(String tableName) {
    super(tableName);
  }
}
