package com.databricks.zerobus.auth;

import com.databricks.zerobus.NonRetriableException;
import java.util.HashMap;
import java.util.Map;

/**
 * Default OAuth 2.0 headers provider for Databricks Zerobus authentication.
 *
 * <p>This provider implements the OAuth 2.0 Client Credentials flow with Unity Catalog privileges.
 * It obtains access tokens from the Databricks OIDC endpoint and includes them in the authorization
 * header along with the target table name.
 *
 * <p>The provider validates that the table name follows the three-part format
 * (catalog.schema.table) and requests appropriate Unity Catalog privileges:
 *
 * <ul>
 *   <li>USE CATALOG on the table's catalog
 *   <li>USE SCHEMA on the table's schema
 *   <li>SELECT and MODIFY on the target table
 * </ul>
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * HeadersProvider provider = new OAuthHeadersProvider(
 *     "catalog.schema.table",
 *     "workspace-id",
 *     "https://workspace.databricks.com",
 *     "client-id",
 *     "client-secret"
 * );
 *
 * ZerobusStream<MyRecord> stream = sdk.createStream(
 *     tableProperties,
 *     clientId,
 *     clientSecret,
 *     options,
 *     provider
 * ).join();
 * }</pre>
 *
 * <p><b>Note:</b> Tokens are fetched lazily when {@link #getHeaders()} is called, not during
 * provider construction. This allows the provider to be created early in the application lifecycle
 * without triggering authentication immediately.
 *
 * @see HeadersProvider
 * @see TokenFactory
 */
public class OAuthHeadersProvider implements HeadersProvider {

  private static final String SDK_VERSION = "0.2.0";
  private static final String USER_AGENT = "zerobus-sdk-java/" + SDK_VERSION;

  private static final String AUTHORIZATION_HEADER = "authorization";
  private static final String TABLE_NAME_HEADER = "x-databricks-zerobus-table-name";
  private static final String USER_AGENT_HEADER = "user-agent";
  private static final String BEARER_PREFIX = "Bearer ";

  private final String tableName;
  private final String workspaceId;
  private final String workspaceUrl;
  private final String clientId;
  private final String clientSecret;

  /**
   * Creates a new OAuth headers provider.
   *
   * @param tableName The fully qualified table name (catalog.schema.table)
   * @param workspaceId The Databricks workspace ID
   * @param workspaceUrl The Unity Catalog endpoint URL
   * @param clientId The OAuth client ID
   * @param clientSecret The OAuth client secret
   */
  public OAuthHeadersProvider(
      String tableName,
      String workspaceId,
      String workspaceUrl,
      String clientId,
      String clientSecret) {
    this.tableName = tableName;
    this.workspaceId = workspaceId;
    this.workspaceUrl = workspaceUrl;
    this.clientId = clientId;
    this.clientSecret = clientSecret;
  }

  /**
   * Returns headers for OAuth 2.0 authentication.
   *
   * <p>This method fetches a fresh OAuth token from the Databricks OIDC endpoint and returns
   * headers containing:
   *
   * <ul>
   *   <li>{@code authorization}: Bearer token for authentication
   *   <li>{@code x-databricks-zerobus-table-name}: Target table name
   *   <li>{@code user-agent}: SDK version identifier (zerobus-sdk-java/version)
   * </ul>
   *
   * @return A map containing the authorization, table name, and user-agent headers
   * @throws NonRetriableException if the table name is invalid or token request fails
   */
  @Override
  public Map<String, String> getHeaders() throws NonRetriableException {
    String token =
        TokenFactory.getZerobusToken(tableName, workspaceId, workspaceUrl, clientId, clientSecret);

    Map<String, String> headers = new HashMap<>();
    headers.put(AUTHORIZATION_HEADER, BEARER_PREFIX + token);
    headers.put(TABLE_NAME_HEADER, tableName);
    headers.put(USER_AGENT_HEADER, USER_AGENT);

    return headers;
  }
}
