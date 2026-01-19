package com.databricks.zerobus.auth;

import com.databricks.zerobus.NonRetriableException;
import java.util.Map;

/**
 * Interface for providing custom headers for gRPC stream authentication and configuration.
 *
 * <p>This interface allows users to implement custom authentication strategies or add additional
 * headers to gRPC requests. The headers returned by {@link #getHeaders()} are attached to all
 * outgoing gRPC requests for the stream.
 *
 * <p>The {@code getHeaders()} method is called:
 *
 * <ul>
 *   <li>Once when creating a stream
 *   <li>When recovering from failure (if recovery is enabled)
 *   <li>When recreating a stream
 * </ul>
 *
 * <p>Example usage with custom headers:
 *
 * <pre>{@code
 * public class CustomHeadersProvider implements HeadersProvider {
 *     private final String token;
 *     private final String tableName;
 *
 *     public CustomHeadersProvider(String token, String tableName) {
 *         this.token = token;
 *         this.tableName = tableName;
 *     }
 *
 *     @Override
 *     public Map<String, String> getHeaders() throws NonRetriableException {
 *         Map<String, String> headers = new HashMap<>();
 *         headers.put("authorization", "Bearer " + token);
 *         headers.put("x-databricks-zerobus-table-name", tableName);
 *         headers.put("x-custom-header", "custom-value");
 *         return headers;
 *     }
 * }
 *
 * HeadersProvider provider = new CustomHeadersProvider(token, tableName);
 * ZerobusStream<MyRecord> stream = sdk.createStream(
 *     tableProperties,
 *     clientId,
 *     clientSecret,
 *     options,
 *     provider
 * ).join();
 * }</pre>
 *
 * @see OAuthHeadersProvider
 * @see com.databricks.zerobus.ZerobusSdk#createStream
 */
public interface HeadersProvider {

  /**
   * Returns headers to be attached to gRPC requests.
   *
   * <p>This method is called synchronously when creating or recreating a stream. It should return a
   * map of header names to header values. Common headers include:
   *
   * <ul>
   *   <li>{@code authorization} - Authentication token (e.g., "Bearer <token>")
   *   <li>{@code x-databricks-zerobus-table-name} - Target table name
   * </ul>
   *
   * <p><b>Important:</b> This method should be thread-safe if the same provider instance is used
   * across multiple streams.
   *
   * @return A map of header names to header values
   * @throws NonRetriableException if headers cannot be obtained due to a non-retriable error (e.g.,
   *     invalid credentials, configuration error). This will cause stream creation to fail without
   *     retry.
   */
  Map<String, String> getHeaders() throws NonRetriableException;
}
