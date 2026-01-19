package com.databricks.zerobus.auth;

import static org.junit.jupiter.api.Assertions.*;

import com.databricks.zerobus.NonRetriableException;
import com.databricks.zerobus.common.http.HttpClient;
import java.io.IOException;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Tests for TokenFactory validation logic.
 *
 * <p>These tests verify parameter validation without making actual HTTP calls.
 */
class TokenFactoryTest {

  private static final String VALID_TABLE_NAME = "catalog.schema.table";
  private static final String VALID_WORKSPACE_ID = "workspace-123";
  private static final String VALID_WORKSPACE_URL = "https://workspace.databricks.com";
  private static final String VALID_CLIENT_ID = "client-id";
  private static final String VALID_CLIENT_SECRET = "client-secret";

  // ==================== Mock HttpClient ====================

  /** Mock HttpClient that returns a configurable response. */
  private static class MockHttpClient implements HttpClient {
    private HttpResponse response;
    private IOException exception;
    private String lastUrl;
    private String lastFormData;
    private Map<String, String> lastHeaders;

    void setResponse(HttpResponse response) {
      this.response = response;
      this.exception = null;
    }

    void setException(IOException exception) {
      this.exception = exception;
      this.response = null;
    }

    @Override
    public HttpResponse post(String url, String formData, Map<String, String> headers)
        throws IOException {
      this.lastUrl = url;
      this.lastFormData = formData;
      this.lastHeaders = headers;
      if (exception != null) {
        throw exception;
      }
      return response;
    }

    @Override
    public HttpResponse get(String url, Map<String, String> headers) throws IOException {
      this.lastUrl = url;
      this.lastHeaders = headers;
      if (exception != null) {
        throw exception;
      }
      return response;
    }
  }

  // ==================== Table Name Validation ====================

  @Test
  void testNullTableName() {
    NonRetriableException ex =
        assertThrows(
            NonRetriableException.class,
            () ->
                TokenFactory.getZerobusToken(
                    null,
                    VALID_WORKSPACE_ID,
                    VALID_WORKSPACE_URL,
                    VALID_CLIENT_ID,
                    VALID_CLIENT_SECRET));
    assertTrue(ex.getMessage().contains("tableName cannot be null"));
  }

  @Test
  void testEmptyTableName() {
    NonRetriableException ex =
        assertThrows(
            NonRetriableException.class,
            () ->
                TokenFactory.getZerobusToken(
                    "",
                    VALID_WORKSPACE_ID,
                    VALID_WORKSPACE_URL,
                    VALID_CLIENT_ID,
                    VALID_CLIENT_SECRET));
    assertTrue(ex.getMessage().contains("tableName cannot be blank"));
  }

  @Test
  void testBlankTableName() {
    NonRetriableException ex =
        assertThrows(
            NonRetriableException.class,
            () ->
                TokenFactory.getZerobusToken(
                    "   ",
                    VALID_WORKSPACE_ID,
                    VALID_WORKSPACE_URL,
                    VALID_CLIENT_ID,
                    VALID_CLIENT_SECRET));
    assertTrue(ex.getMessage().contains("tableName cannot be blank"));
  }

  @Test
  void testTableNameSinglePart() {
    NonRetriableException ex =
        assertThrows(
            NonRetriableException.class,
            () ->
                TokenFactory.getZerobusToken(
                    "table",
                    VALID_WORKSPACE_ID,
                    VALID_WORKSPACE_URL,
                    VALID_CLIENT_ID,
                    VALID_CLIENT_SECRET));
    assertTrue(ex.getMessage().contains("must be in the format of catalog.schema.table"));
  }

  @Test
  void testTableNameTwoParts() {
    NonRetriableException ex =
        assertThrows(
            NonRetriableException.class,
            () ->
                TokenFactory.getZerobusToken(
                    "schema.table",
                    VALID_WORKSPACE_ID,
                    VALID_WORKSPACE_URL,
                    VALID_CLIENT_ID,
                    VALID_CLIENT_SECRET));
    assertTrue(ex.getMessage().contains("must be in the format of catalog.schema.table"));
  }

  @Test
  void testTableNameFourParts() {
    NonRetriableException ex =
        assertThrows(
            NonRetriableException.class,
            () ->
                TokenFactory.getZerobusToken(
                    "a.b.c.d",
                    VALID_WORKSPACE_ID,
                    VALID_WORKSPACE_URL,
                    VALID_CLIENT_ID,
                    VALID_CLIENT_SECRET));
    assertTrue(ex.getMessage().contains("must be in the format of catalog.schema.table"));
  }

  @Test
  void testTableNameEmptyCatalog() {
    NonRetriableException ex =
        assertThrows(
            NonRetriableException.class,
            () ->
                TokenFactory.getZerobusToken(
                    ".schema.table",
                    VALID_WORKSPACE_ID,
                    VALID_WORKSPACE_URL,
                    VALID_CLIENT_ID,
                    VALID_CLIENT_SECRET));
    assertTrue(ex.getMessage().contains("empty parts"));
  }

  @Test
  void testTableNameEmptySchema() {
    NonRetriableException ex =
        assertThrows(
            NonRetriableException.class,
            () ->
                TokenFactory.getZerobusToken(
                    "catalog..table",
                    VALID_WORKSPACE_ID,
                    VALID_WORKSPACE_URL,
                    VALID_CLIENT_ID,
                    VALID_CLIENT_SECRET));
    assertTrue(ex.getMessage().contains("empty parts"));
  }

  @Test
  void testTableNameEmptyTable() {
    NonRetriableException ex =
        assertThrows(
            NonRetriableException.class,
            () ->
                TokenFactory.getZerobusToken(
                    "catalog.schema.",
                    VALID_WORKSPACE_ID,
                    VALID_WORKSPACE_URL,
                    VALID_CLIENT_ID,
                    VALID_CLIENT_SECRET));
    assertTrue(ex.getMessage().contains("empty parts"));
  }

  // ==================== Workspace ID Validation ====================

  @Test
  void testNullWorkspaceId() {
    NonRetriableException ex =
        assertThrows(
            NonRetriableException.class,
            () ->
                TokenFactory.getZerobusToken(
                    VALID_TABLE_NAME,
                    null,
                    VALID_WORKSPACE_URL,
                    VALID_CLIENT_ID,
                    VALID_CLIENT_SECRET));
    assertTrue(ex.getMessage().contains("workspaceId cannot be null"));
  }

  @Test
  void testEmptyWorkspaceId() {
    NonRetriableException ex =
        assertThrows(
            NonRetriableException.class,
            () ->
                TokenFactory.getZerobusToken(
                    VALID_TABLE_NAME,
                    "",
                    VALID_WORKSPACE_URL,
                    VALID_CLIENT_ID,
                    VALID_CLIENT_SECRET));
    assertTrue(ex.getMessage().contains("workspaceId cannot be blank"));
  }

  @Test
  void testBlankWorkspaceId() {
    NonRetriableException ex =
        assertThrows(
            NonRetriableException.class,
            () ->
                TokenFactory.getZerobusToken(
                    VALID_TABLE_NAME,
                    "   ",
                    VALID_WORKSPACE_URL,
                    VALID_CLIENT_ID,
                    VALID_CLIENT_SECRET));
    assertTrue(ex.getMessage().contains("workspaceId cannot be blank"));
  }

  // ==================== Workspace URL Validation ====================

  @Test
  void testNullWorkspaceUrl() {
    NonRetriableException ex =
        assertThrows(
            NonRetriableException.class,
            () ->
                TokenFactory.getZerobusToken(
                    VALID_TABLE_NAME,
                    VALID_WORKSPACE_ID,
                    null,
                    VALID_CLIENT_ID,
                    VALID_CLIENT_SECRET));
    assertTrue(ex.getMessage().contains("workspaceUrl cannot be null"));
  }

  @Test
  void testEmptyWorkspaceUrl() {
    NonRetriableException ex =
        assertThrows(
            NonRetriableException.class,
            () ->
                TokenFactory.getZerobusToken(
                    VALID_TABLE_NAME,
                    VALID_WORKSPACE_ID,
                    "",
                    VALID_CLIENT_ID,
                    VALID_CLIENT_SECRET));
    assertTrue(ex.getMessage().contains("workspaceUrl cannot be blank"));
  }

  @Test
  void testBlankWorkspaceUrl() {
    NonRetriableException ex =
        assertThrows(
            NonRetriableException.class,
            () ->
                TokenFactory.getZerobusToken(
                    VALID_TABLE_NAME,
                    VALID_WORKSPACE_ID,
                    "   ",
                    VALID_CLIENT_ID,
                    VALID_CLIENT_SECRET));
    assertTrue(ex.getMessage().contains("workspaceUrl cannot be blank"));
  }

  // ==================== Client ID Validation ====================

  @Test
  void testNullClientId() {
    NonRetriableException ex =
        assertThrows(
            NonRetriableException.class,
            () ->
                TokenFactory.getZerobusToken(
                    VALID_TABLE_NAME,
                    VALID_WORKSPACE_ID,
                    VALID_WORKSPACE_URL,
                    null,
                    VALID_CLIENT_SECRET));
    assertTrue(ex.getMessage().contains("clientId cannot be null"));
  }

  @Test
  void testEmptyClientId() {
    NonRetriableException ex =
        assertThrows(
            NonRetriableException.class,
            () ->
                TokenFactory.getZerobusToken(
                    VALID_TABLE_NAME,
                    VALID_WORKSPACE_ID,
                    VALID_WORKSPACE_URL,
                    "",
                    VALID_CLIENT_SECRET));
    assertTrue(ex.getMessage().contains("clientId cannot be blank"));
  }

  @Test
  void testBlankClientId() {
    NonRetriableException ex =
        assertThrows(
            NonRetriableException.class,
            () ->
                TokenFactory.getZerobusToken(
                    VALID_TABLE_NAME,
                    VALID_WORKSPACE_ID,
                    VALID_WORKSPACE_URL,
                    "   ",
                    VALID_CLIENT_SECRET));
    assertTrue(ex.getMessage().contains("clientId cannot be blank"));
  }

  // ==================== Client Secret Validation ====================

  @Test
  void testNullClientSecret() {
    NonRetriableException ex =
        assertThrows(
            NonRetriableException.class,
            () ->
                TokenFactory.getZerobusToken(
                    VALID_TABLE_NAME,
                    VALID_WORKSPACE_ID,
                    VALID_WORKSPACE_URL,
                    VALID_CLIENT_ID,
                    null));
    assertTrue(ex.getMessage().contains("clientSecret cannot be null"));
  }

  @Test
  void testEmptyClientSecret() {
    NonRetriableException ex =
        assertThrows(
            NonRetriableException.class,
            () ->
                TokenFactory.getZerobusToken(
                    VALID_TABLE_NAME,
                    VALID_WORKSPACE_ID,
                    VALID_WORKSPACE_URL,
                    VALID_CLIENT_ID,
                    ""));
    assertTrue(ex.getMessage().contains("clientSecret cannot be blank"));
  }

  @Test
  void testBlankClientSecret() {
    NonRetriableException ex =
        assertThrows(
            NonRetriableException.class,
            () ->
                TokenFactory.getZerobusToken(
                    VALID_TABLE_NAME,
                    VALID_WORKSPACE_ID,
                    VALID_WORKSPACE_URL,
                    VALID_CLIENT_ID,
                    "   "));
    assertTrue(ex.getMessage().contains("clientSecret cannot be blank"));
  }

  // ==================== parseTableName Tests ====================

  @Test
  void testParseTableNameValid() throws NonRetriableException {
    String[] parts = TokenFactory.parseTableName("catalog.schema.table");
    assertEquals(3, parts.length);
    assertEquals("catalog", parts[0]);
    assertEquals("schema", parts[1]);
    assertEquals("table", parts[2]);
  }

  @Test
  void testParseTableNameWithSpecialChars() throws NonRetriableException {
    String[] parts = TokenFactory.parseTableName("my_catalog.my_schema.my_table");
    assertEquals("my_catalog", parts[0]);
    assertEquals("my_schema", parts[1]);
    assertEquals("my_table", parts[2]);
  }

  // ==================== buildAuthorizationDetails Tests ====================

  @Test
  void testBuildAuthorizationDetails() {
    String details = TokenFactory.buildAuthorizationDetails("cat", "sch", "tbl");

    assertTrue(details.contains("\"object_full_path\": \"cat\""));
    assertTrue(details.contains("\"object_full_path\": \"cat.sch\""));
    assertTrue(details.contains("\"object_full_path\": \"cat.sch.tbl\""));
    assertTrue(details.contains("\"privileges\": [\"USE CATALOG\"]"));
    assertTrue(details.contains("\"privileges\": [\"USE SCHEMA\"]"));
    assertTrue(details.contains("\"privileges\": [\"SELECT\", \"MODIFY\"]"));
  }

  // ==================== extractAccessToken Tests ====================

  @Test
  void testExtractAccessTokenValid() throws NonRetriableException {
    String response = "{\"access_token\": \"my-jwt-token\", \"token_type\": \"Bearer\"}";
    String token = TokenFactory.extractAccessToken(response);
    assertEquals("my-jwt-token", token);
  }

  @Test
  void testExtractAccessTokenWithWhitespace() throws NonRetriableException {
    String response = "{\"access_token\"  :  \"token-with-spaces\" }";
    String token = TokenFactory.extractAccessToken(response);
    assertEquals("token-with-spaces", token);
  }

  @Test
  void testExtractAccessTokenMissing() {
    String response = "{\"token_type\": \"Bearer\"}";
    NonRetriableException ex =
        assertThrows(NonRetriableException.class, () -> TokenFactory.extractAccessToken(response));
    assertTrue(ex.getMessage().contains("No access token received"));
  }

  @Test
  void testExtractAccessTokenNullResponse() {
    NonRetriableException ex =
        assertThrows(NonRetriableException.class, () -> TokenFactory.extractAccessToken(null));
    assertTrue(ex.getMessage().contains("No response body received"));
  }

  // ==================== HTTP Client Tests ====================

  @Test
  void testGetZerobusTokenSuccess() throws NonRetriableException {
    MockHttpClient mockClient = new MockHttpClient();
    mockClient.setResponse(
        new HttpClient.HttpResponse(200, "{\"access_token\": \"test-token\"}", null));

    String token =
        TokenFactory.getZerobusToken(
            VALID_TABLE_NAME,
            VALID_WORKSPACE_ID,
            VALID_WORKSPACE_URL,
            VALID_CLIENT_ID,
            VALID_CLIENT_SECRET,
            mockClient);

    assertEquals("test-token", token);
    assertEquals(VALID_WORKSPACE_URL + "/oidc/v1/token", mockClient.lastUrl);
    assertTrue(mockClient.lastFormData.contains("grant_type=client_credentials"));
    assertTrue(mockClient.lastFormData.contains("scope=all-apis"));
    assertTrue(mockClient.lastHeaders.containsKey("Authorization"));
    assertTrue(mockClient.lastHeaders.get("Authorization").startsWith("Basic "));
  }

  @Test
  void testGetZerobusTokenHttpError() {
    MockHttpClient mockClient = new MockHttpClient();
    mockClient.setResponse(new HttpClient.HttpResponse(401, null, "Unauthorized"));

    NonRetriableException ex =
        assertThrows(
            NonRetriableException.class,
            () ->
                TokenFactory.getZerobusToken(
                    VALID_TABLE_NAME,
                    VALID_WORKSPACE_ID,
                    VALID_WORKSPACE_URL,
                    VALID_CLIENT_ID,
                    VALID_CLIENT_SECRET,
                    mockClient));

    assertTrue(ex.getMessage().contains("OAuth request failed with status 401"));
    assertTrue(ex.getMessage().contains("Unauthorized"));
  }

  @Test
  void testGetZerobusTokenHttpException() {
    MockHttpClient mockClient = new MockHttpClient();
    mockClient.setException(new IOException("Connection refused"));

    NonRetriableException ex =
        assertThrows(
            NonRetriableException.class,
            () ->
                TokenFactory.getZerobusToken(
                    VALID_TABLE_NAME,
                    VALID_WORKSPACE_ID,
                    VALID_WORKSPACE_URL,
                    VALID_CLIENT_ID,
                    VALID_CLIENT_SECRET,
                    mockClient));

    assertTrue(ex.getMessage().contains("Unexpected error getting OAuth token"));
  }

  @Test
  void testGetZerobusTokenNoTokenInResponse() {
    MockHttpClient mockClient = new MockHttpClient();
    mockClient.setResponse(new HttpClient.HttpResponse(200, "{\"error\": \"no token\"}", null));

    NonRetriableException ex =
        assertThrows(
            NonRetriableException.class,
            () ->
                TokenFactory.getZerobusToken(
                    VALID_TABLE_NAME,
                    VALID_WORKSPACE_ID,
                    VALID_WORKSPACE_URL,
                    VALID_CLIENT_ID,
                    VALID_CLIENT_SECRET,
                    mockClient));

    assertTrue(ex.getMessage().contains("No access token received"));
  }
}
