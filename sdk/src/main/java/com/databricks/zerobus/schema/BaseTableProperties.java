package com.databricks.zerobus.schema;

/**
 * Base class for table properties.
 *
 * <p>This abstract class defines the common properties for all table property implementations,
 * including proto-based and JSON table properties.
 *
 * @see ProtoTableProperties
 * @see JsonTableProperties
 */
public abstract class BaseTableProperties {

  private final String tableName;

  /**
   * Creates a new table properties instance.
   *
   * @param tableName the fully qualified table name (catalog.schema.table)
   */
  protected BaseTableProperties(String tableName) {
    this.tableName = tableName;
  }

  /**
   * Returns the fully qualified table name.
   *
   * <p>The table name should be in the format: catalog.schema.table
   *
   * @return the fully qualified table name
   */
  public String getTableName() {
    return tableName;
  }
}
