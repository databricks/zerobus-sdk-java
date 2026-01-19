package com.databricks.zerobus;

import static org.junit.jupiter.api.Assertions.*;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Tests for ZerobusSdkStubFactory validation logic. */
class ZerobusSdkStubFactoryTest {

  @Test
  void testValidateTableNameHeader_Success() throws NonRetriableException {
    Map<String, String> headers = new HashMap<>();
    headers.put("x-databricks-zerobus-table-name", "catalog.schema.table");
    headers.put("authorization", "Bearer token");

    // Should not throw
    ZerobusSdkStubFactory.validateTableNameHeader(headers, "catalog.schema.table");
  }

  @Test
  void testValidateTableNameHeader_MissingHeader() {
    Map<String, String> headers = new HashMap<>();
    headers.put("authorization", "Bearer token");
    // Missing x-databricks-zerobus-table-name header

    NonRetriableException exception =
        assertThrows(
            NonRetriableException.class,
            () -> ZerobusSdkStubFactory.validateTableNameHeader(headers, "catalog.schema.table"));

    assertTrue(exception.getMessage().contains("must include"));
    assertTrue(exception.getMessage().contains("x-databricks-zerobus-table-name"));
  }

  @Test
  void testValidateTableNameHeader_Mismatch() {
    Map<String, String> headers = new HashMap<>();
    headers.put("x-databricks-zerobus-table-name", "wrong.table.name");
    headers.put("authorization", "Bearer token");

    NonRetriableException exception =
        assertThrows(
            NonRetriableException.class,
            () -> ZerobusSdkStubFactory.validateTableNameHeader(headers, "catalog.schema.table"));

    assertTrue(exception.getMessage().contains("Table name mismatch"));
    assertTrue(exception.getMessage().contains("wrong.table.name"));
    assertTrue(exception.getMessage().contains("catalog.schema.table"));
  }

  @Test
  void testValidateTableNameHeader_EmptyTableName() {
    Map<String, String> headers = new HashMap<>();
    headers.put("x-databricks-zerobus-table-name", "");
    headers.put("authorization", "Bearer token");

    NonRetriableException exception =
        assertThrows(
            NonRetriableException.class,
            () -> ZerobusSdkStubFactory.validateTableNameHeader(headers, "catalog.schema.table"));

    assertTrue(exception.getMessage().contains("Table name mismatch"));
  }

  @Test
  void testValidateTableNameHeader_CaseSensitive() {
    Map<String, String> headers = new HashMap<>();
    headers.put("x-databricks-zerobus-table-name", "Catalog.Schema.Table");

    NonRetriableException exception =
        assertThrows(
            NonRetriableException.class,
            () -> ZerobusSdkStubFactory.validateTableNameHeader(headers, "catalog.schema.table"));

    assertTrue(exception.getMessage().contains("Table name mismatch"));
  }

  @Test
  void testTableNameHeaderConstant() {
    assertEquals("x-databricks-zerobus-table-name", ZerobusSdkStubFactory.TABLE_NAME_HEADER);
  }
}
