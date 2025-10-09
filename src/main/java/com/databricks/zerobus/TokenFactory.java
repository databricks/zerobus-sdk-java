package com.databricks.zerobus;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
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
  public static String getZerobusToken(
      String tableName,
      String workspaceId,
      String workspaceUrl,
      String clientId,
      String clientSecret)
      throws NonRetriableException {

    // Parse and validate the three-part table name
    String[] threePartTableName = tableName.split("\\.");
    if (threePartTableName.length != 3) {
      throw new NonRetriableException(
          "Table name '" + tableName + "' must be in the format of catalog.schema.table");
    }

    String catalogName = threePartTableName[0];
    String schemaName = threePartTableName[1];
    String tableNameOnly = threePartTableName[2];

    // Build authorization details using the RAR (RFC 9396) format.
    // Newlines are required for proper JWT claim formatting.
    String authorizationDetails =
        String.format(
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
            catalogName, catalogName, schemaName, catalogName, schemaName, tableNameOnly);

    String urlString = workspaceUrl + "/oidc/v1/token";

    try {
      // Build OAuth 2.0 client credentials request with Unity Catalog authorization details
      String formData =
          "grant_type=client_credentials"
              + "&scope=all-apis"
              + "&resource=api://databricks/workspaces/"
              + workspaceId
              + "/zerobusDirectWriteApi"
              + "&authorization_details="
              + URLEncoder.encode(authorizationDetails, "UTF-8");

      // Encode credentials for HTTP Basic authentication
      String credentials =
          Base64.getEncoder()
              .encodeToString((clientId + ":" + clientSecret).getBytes(StandardCharsets.UTF_8));

      HttpURLConnection connection = (HttpURLConnection) new URL(urlString).openConnection();
      connection.setRequestMethod("POST");
      connection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
      connection.setRequestProperty("Authorization", "Basic " + credentials);
      connection.setDoOutput(true);

      OutputStreamWriter writer =
          new OutputStreamWriter(connection.getOutputStream(), StandardCharsets.UTF_8);
      writer.write(formData);
      writer.close();

      int responseCode = connection.getResponseCode();

      if (responseCode != 200) {
        String errorBody = "No error details available";
        if (connection.getErrorStream() != null) {
          BufferedReader errorReader =
              new BufferedReader(
                  new InputStreamReader(connection.getErrorStream(), StandardCharsets.UTF_8));
          StringBuilder errorBuilder = new StringBuilder();
          String line;
          while ((line = errorReader.readLine()) != null) {
            errorBuilder.append(line).append("\n");
          }
          errorReader.close();
          errorBody = errorBuilder.toString();
        }
        throw new NonRetriableException(
            "OAuth request failed with status " + responseCode + ": " + errorBody);
      }

      BufferedReader reader =
          new BufferedReader(
              new InputStreamReader(connection.getInputStream(), StandardCharsets.UTF_8));
      StringBuilder responseBody = new StringBuilder();
      String line;
      while ((line = reader.readLine()) != null) {
        responseBody.append(line).append("\n");
      }
      reader.close();

      // Extract access token using regex to avoid dependency on a JSON library.
      // Pattern matches: "access_token": "value" with flexible whitespace.
      Pattern accessTokenPattern = Pattern.compile("\"access_token\"\\s*:\\s*\"([^\"]+)\"");
      Matcher matcher = accessTokenPattern.matcher(responseBody.toString());

      if (matcher.find()) {
        return matcher.group(1);
      } else {
        throw new NonRetriableException("No access token received from OAuth response");
      }
    } catch (NonRetriableException e) {
      throw e;
    } catch (Exception e) {
      throw new NonRetriableException("Unexpected error getting OAuth token: " + e.getMessage(), e);
    }
  }
}
