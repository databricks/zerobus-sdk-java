package com.databricks.zerobus.auth;

import com.databricks.zerobus.NonRetriableException;
import com.databricks.zerobus.common.http.DefaultHttpClient;
import com.databricks.zerobus.common.http.HttpClient;
import com.databricks.zerobus.common.http.RetryingHttpClient;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Factory for obtaining OAuth 2.0 access tokens with Unity Catalog privileges.
 *
 * <p>This class uses the OAuth 2.0 client credentials flow with authorization details to request
 * tokens scoped to specific Unity Catalog resources. The generated tokens include privileges for
 * catalog, schema, and table access required for ingestion.
 */
public class TokenFactory {

  private static final Pattern ACCESS_TOKEN_PATTERN =
      Pattern.compile("\"access_token\"\\s*:\\s*\"([^\"]+)\"");

  /**
   * Obtains an OAuth token with Unity Catalog privileges for the specified table.
   *
   * <p>The token request includes authorization details that grant:
   *
   * <ul>
   *   <li>USE CATALOG on the table's catalog
   *   <li>USE SCHEMA on the table's schema
   *   <li>SELECT and MODIFY on the target table
   * </ul>
   *
   * @param tableName The fully qualified table name (catalog.schema.table)
   * @param workspaceId The Databricks workspace ID
   * @param workspaceUrl The Unity Catalog endpoint URL
   * @param clientId The OAuth client ID
   * @param clientSecret The OAuth client secret
   * @return The OAuth access token (JWT)
   * @throws NonRetriableException if the token request fails or table name is invalid
   */
  private static final HttpClient DEFAULT_HTTP_CLIENT =
      new RetryingHttpClient(DefaultHttpClient.INSTANCE);

  public static String getZerobusToken(
      String tableName,
      String workspaceId,
      String workspaceUrl,
      String clientId,
      String clientSecret)
      throws NonRetriableException {
    return getZerobusToken(
        tableName, workspaceId, workspaceUrl, clientId, clientSecret, DEFAULT_HTTP_CLIENT);
  }

  /**
   * Obtains an OAuth token with Unity Catalog privileges for the specified table.
   *
   * <p>This overload accepts an {@link HttpClient} for testing purposes.
   *
   * @param tableName The fully qualified table name (catalog.schema.table)
   * @param workspaceId The Databricks workspace ID
   * @param workspaceUrl The Unity Catalog endpoint URL
   * @param clientId The OAuth client ID
   * @param clientSecret The OAuth client secret
   * @param httpClient The HTTP client to use for requests
   * @return The OAuth access token (JWT)
   * @throws NonRetriableException if the token request fails or table name is invalid
   */
  public static String getZerobusToken(
      String tableName,
      String workspaceId,
      String workspaceUrl,
      String clientId,
      String clientSecret,
      HttpClient httpClient)
      throws NonRetriableException {

    validateNotBlank(tableName, "tableName");
    validateNotBlank(workspaceId, "workspaceId");
    validateNotBlank(workspaceUrl, "workspaceUrl");
    validateNotBlank(clientId, "clientId");
    validateNotBlank(clientSecret, "clientSecret");

    String[] threePartTableName = parseTableName(tableName);
    String catalogName = threePartTableName[0];
    String schemaName = threePartTableName[1];
    String tableNameOnly = threePartTableName[2];

    String authorizationDetails = buildAuthorizationDetails(catalogName, schemaName, tableNameOnly);

    String url = workspaceUrl + "/oidc/v1/token";

    try {
      String formData =
          "grant_type=client_credentials"
              + "&scope=all-apis"
              + "&resource=api://databricks/workspaces/"
              + workspaceId
              + "/zerobusDirectWriteApi"
              + "&authorization_details="
              + URLEncoder.encode(authorizationDetails, "UTF-8");

      String credentials =
          Base64.getEncoder()
              .encodeToString((clientId + ":" + clientSecret).getBytes(StandardCharsets.UTF_8));

      Map<String, String> headers = new HashMap<>();
      headers.put("Authorization", "Basic " + credentials);

      HttpClient.HttpResponse response = httpClient.post(url, formData, headers);

      if (!response.isSuccess()) {
        String errorBody =
            response.getErrorBody() != null
                ? response.getErrorBody()
                : "No error details available";
        throw new NonRetriableException(
            "OAuth request failed with status " + response.getStatusCode() + ": " + errorBody);
      }

      return extractAccessToken(response.getBody());
    } catch (NonRetriableException e) {
      throw e;
    } catch (Exception e) {
      throw new NonRetriableException("Unexpected error getting OAuth token: " + e.getMessage(), e);
    }
  }

  /**
   * Validates that a parameter is not null or blank.
   *
   * @param value The value to validate
   * @param paramName The parameter name for error messages
   * @throws NonRetriableException if the value is null or blank
   */
  static void validateNotBlank(String value, String paramName) throws NonRetriableException {
    if (value == null) {
      throw new NonRetriableException(paramName + " cannot be null");
    }
    if (value.trim().isEmpty()) {
      throw new NonRetriableException(paramName + " cannot be blank");
    }
  }

  /**
   * Parses and validates a three-part table name.
   *
   * @param tableName The table name in format catalog.schema.table
   * @return Array of [catalog, schema, table]
   * @throws NonRetriableException if the table name format is invalid
   */
  static String[] parseTableName(String tableName) throws NonRetriableException {
    String[] parts = tableName.split("\\.", -1);
    if (parts.length != 3) {
      throw new NonRetriableException(
          "Table name '" + tableName + "' must be in the format of catalog.schema.table");
    }

    for (int i = 0; i < 3; i++) {
      if (parts[i].trim().isEmpty()) {
        throw new NonRetriableException(
            "Table name '" + tableName + "' contains empty parts; must be catalog.schema.table");
      }
    }

    return parts;
  }

  /**
   * Builds the authorization details JSON for Unity Catalog privileges.
   *
   * @param catalogName The catalog name
   * @param schemaName The schema name
   * @param tableName The table name (without catalog/schema prefix)
   * @return The authorization details JSON string
   */
  static String buildAuthorizationDetails(String catalogName, String schemaName, String tableName) {
    return String.format(
        "[\n"
            + "  {\n"
            + "    \"type\": \"unity_catalog_privileges\",\n"
            + "    \"privileges\": [\"USE CATALOG\"],\n"
            + "    \"object_type\": \"CATALOG\",\n"
            + "    \"object_full_path\": \"%s\"\n"
            + "  },\n"
            + "  {\n"
            + "    \"type\": \"unity_catalog_privileges\",\n"
            + "    \"privileges\": [\"USE SCHEMA\"],\n"
            + "    \"object_type\": \"SCHEMA\",\n"
            + "    \"object_full_path\": \"%s.%s\"\n"
            + "  },\n"
            + "  {\n"
            + "    \"type\": \"unity_catalog_privileges\",\n"
            + "    \"privileges\": [\"SELECT\", \"MODIFY\"],\n"
            + "    \"object_type\": \"TABLE\",\n"
            + "    \"object_full_path\": \"%s.%s.%s\"\n"
            + "  }\n"
            + "]",
        catalogName, catalogName, schemaName, catalogName, schemaName, tableName);
  }

  /**
   * Extracts the access token from an OAuth response body.
   *
   * @param responseBody The OAuth response body
   * @return The access token
   * @throws NonRetriableException if no access token is found
   */
  static String extractAccessToken(String responseBody) throws NonRetriableException {
    if (responseBody == null) {
      throw new NonRetriableException("No response body received from OAuth request");
    }
    Matcher matcher = ACCESS_TOKEN_PATTERN.matcher(responseBody);
    if (matcher.find()) {
      return matcher.group(1);
    } else {
      throw new NonRetriableException("No access token received from OAuth response");
    }
  }
}
