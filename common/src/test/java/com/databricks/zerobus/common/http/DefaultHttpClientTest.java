package com.databricks.zerobus.common.http;

import static org.junit.jupiter.api.Assertions.*;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for DefaultHttpClient using an embedded HTTP server. */
class DefaultHttpClientTest {

  private HttpServer server;
  private int port;
  private DefaultHttpClient client;

  @BeforeEach
  void setUp() throws IOException {
    server = HttpServer.create(new InetSocketAddress(0), 0);
    port = server.getAddress().getPort();
    server.setExecutor(null);
    server.start();
    client = DefaultHttpClient.INSTANCE;
  }

  @AfterEach
  void tearDown() {
    if (server != null) {
      server.stop(0);
    }
  }

  private String getBaseUrl() {
    return "http://localhost:" + port;
  }

  // ==================== POST Tests ====================

  @Test
  void testPostSuccess() throws IOException {
    server.createContext(
        "/post",
        exchange -> {
          assertEquals("POST", exchange.getRequestMethod());

          String requestBody = readRequestBody(exchange);
          assertEquals("key=value&foo=bar", requestBody);

          String response = "{\"status\": \"ok\"}";
          sendResponse(exchange, 200, response);
        });

    Map<String, String> headers = new HashMap<>();
    HttpClient.HttpResponse response =
        client.post(getBaseUrl() + "/post", "key=value&foo=bar", headers);

    assertEquals(200, response.getStatusCode());
    assertTrue(response.isSuccess());
    assertNotNull(response.getBody());
    assertTrue(response.getBody().contains("\"status\": \"ok\""));
    assertNull(response.getErrorBody());
  }

  @Test
  void testPostWithHeaders() throws IOException {
    server.createContext(
        "/post-headers",
        exchange -> {
          assertEquals("custom-value", exchange.getRequestHeaders().getFirst("X-Custom-Header"));
          assertEquals("Bearer token123", exchange.getRequestHeaders().getFirst("Authorization"));

          sendResponse(exchange, 200, "ok");
        });

    Map<String, String> headers = new HashMap<>();
    headers.put("X-Custom-Header", "custom-value");
    headers.put("Authorization", "Bearer token123");

    HttpClient.HttpResponse response =
        client.post(getBaseUrl() + "/post-headers", "data=test", headers);

    assertEquals(200, response.getStatusCode());
    assertTrue(response.isSuccess());
  }

  @Test
  void testPostClientError() throws IOException {
    server.createContext(
        "/post-error",
        exchange -> {
          String errorBody = "{\"error\": \"Bad Request\"}";
          sendResponse(exchange, 400, errorBody);
        });

    Map<String, String> headers = new HashMap<>();
    HttpClient.HttpResponse response =
        client.post(getBaseUrl() + "/post-error", "invalid=data", headers);

    assertEquals(400, response.getStatusCode());
    assertFalse(response.isSuccess());
    assertNull(response.getBody());
    assertNotNull(response.getErrorBody());
    assertTrue(response.getErrorBody().contains("Bad Request"));
  }

  @Test
  void testPostServerError() throws IOException {
    server.createContext(
        "/post-server-error",
        exchange -> {
          String errorBody = "{\"error\": \"Internal Server Error\"}";
          sendResponse(exchange, 500, errorBody);
        });

    Map<String, String> headers = new HashMap<>();
    HttpClient.HttpResponse response =
        client.post(getBaseUrl() + "/post-server-error", "data=test", headers);

    assertEquals(500, response.getStatusCode());
    assertFalse(response.isSuccess());
    assertNull(response.getBody());
    assertNotNull(response.getErrorBody());
  }

  @Test
  void testPostEmptyResponse() throws IOException {
    server.createContext(
        "/post-empty",
        exchange -> {
          exchange.sendResponseHeaders(204, -1);
          exchange.close();
        });

    Map<String, String> headers = new HashMap<>();
    HttpClient.HttpResponse response =
        client.post(getBaseUrl() + "/post-empty", "data=test", headers);

    assertEquals(204, response.getStatusCode());
    assertTrue(response.isSuccess());
  }

  // ==================== GET Tests ====================

  @Test
  void testGetSuccess() throws IOException {
    server.createContext(
        "/get",
        exchange -> {
          assertEquals("GET", exchange.getRequestMethod());

          String response = "{\"data\": \"hello\"}";
          sendResponse(exchange, 200, response);
        });

    Map<String, String> headers = new HashMap<>();
    HttpClient.HttpResponse response = client.get(getBaseUrl() + "/get", headers);

    assertEquals(200, response.getStatusCode());
    assertTrue(response.isSuccess());
    assertNotNull(response.getBody());
    assertTrue(response.getBody().contains("\"data\": \"hello\""));
    assertNull(response.getErrorBody());
  }

  @Test
  void testGetWithHeaders() throws IOException {
    server.createContext(
        "/get-headers",
        exchange -> {
          assertEquals("application/json", exchange.getRequestHeaders().getFirst("Accept"));
          assertEquals("Bearer mytoken", exchange.getRequestHeaders().getFirst("Authorization"));

          sendResponse(exchange, 200, "ok");
        });

    Map<String, String> headers = new HashMap<>();
    headers.put("Accept", "application/json");
    headers.put("Authorization", "Bearer mytoken");

    HttpClient.HttpResponse response = client.get(getBaseUrl() + "/get-headers", headers);

    assertEquals(200, response.getStatusCode());
    assertTrue(response.isSuccess());
  }

  @Test
  void testGetNotFound() throws IOException {
    server.createContext(
        "/get-not-found",
        exchange -> {
          String errorBody = "{\"error\": \"Not Found\"}";
          sendResponse(exchange, 404, errorBody);
        });

    Map<String, String> headers = new HashMap<>();
    HttpClient.HttpResponse response = client.get(getBaseUrl() + "/get-not-found", headers);

    assertEquals(404, response.getStatusCode());
    assertFalse(response.isSuccess());
    assertNull(response.getBody());
    assertNotNull(response.getErrorBody());
    assertTrue(response.getErrorBody().contains("Not Found"));
  }

  @Test
  void testGetUnauthorized() throws IOException {
    server.createContext(
        "/get-unauthorized",
        exchange -> {
          String errorBody = "{\"error\": \"Unauthorized\"}";
          sendResponse(exchange, 401, errorBody);
        });

    Map<String, String> headers = new HashMap<>();
    HttpClient.HttpResponse response = client.get(getBaseUrl() + "/get-unauthorized", headers);

    assertEquals(401, response.getStatusCode());
    assertFalse(response.isSuccess());
  }

  // ==================== HttpResponse Tests ====================

  @Test
  void testHttpResponseIsSuccess() {
    // 2xx are success
    assertTrue(new HttpClient.HttpResponse(200, "body", null).isSuccess());
    assertTrue(new HttpClient.HttpResponse(201, "body", null).isSuccess());
    assertTrue(new HttpClient.HttpResponse(204, null, null).isSuccess());
    assertTrue(new HttpClient.HttpResponse(299, "body", null).isSuccess());

    // < 200 are not success
    assertFalse(new HttpClient.HttpResponse(199, null, "error").isSuccess());

    // >= 300 are not success
    assertFalse(new HttpClient.HttpResponse(300, null, "error").isSuccess());
    assertFalse(new HttpClient.HttpResponse(400, null, "error").isSuccess());
    assertFalse(new HttpClient.HttpResponse(500, null, "error").isSuccess());
  }

  @Test
  void testHttpResponseGetters() {
    HttpClient.HttpResponse response = new HttpClient.HttpResponse(200, "body content", null);
    assertEquals(200, response.getStatusCode());
    assertEquals("body content", response.getBody());
    assertNull(response.getErrorBody());

    HttpClient.HttpResponse errorResponse = new HttpClient.HttpResponse(500, null, "error content");
    assertEquals(500, errorResponse.getStatusCode());
    assertNull(errorResponse.getBody());
    assertEquals("error content", errorResponse.getErrorBody());
  }

  // ==================== Edge Cases ====================

  @Test
  void testPostConnectionError() {
    // Try to connect to a port that's not listening
    Map<String, String> headers = new HashMap<>();
    assertThrows(IOException.class, () -> client.post("http://localhost:1", "data=test", headers));
  }

  @Test
  void testGetConnectionError() {
    Map<String, String> headers = new HashMap<>();
    assertThrows(IOException.class, () -> client.get("http://localhost:1", headers));
  }

  @Test
  void testPostEmptyHeaders() throws IOException {
    server.createContext(
        "/post-no-headers",
        exchange -> {
          sendResponse(exchange, 200, "ok");
        });

    HttpClient.HttpResponse response =
        client.post(getBaseUrl() + "/post-no-headers", "data=test", new HashMap<>());

    assertEquals(200, response.getStatusCode());
  }

  @Test
  void testGetEmptyHeaders() throws IOException {
    server.createContext(
        "/get-no-headers",
        exchange -> {
          sendResponse(exchange, 200, "ok");
        });

    HttpClient.HttpResponse response =
        client.get(getBaseUrl() + "/get-no-headers", new HashMap<>());

    assertEquals(200, response.getStatusCode());
  }

  @Test
  void testErrorWithNoBody() throws IOException {
    server.createContext(
        "/error-no-body",
        exchange -> {
          // Send error with no body (empty error stream)
          exchange.sendResponseHeaders(500, -1);
          exchange.close();
        });

    Map<String, String> headers = new HashMap<>();
    HttpClient.HttpResponse response = client.get(getBaseUrl() + "/error-no-body", headers);

    assertEquals(500, response.getStatusCode());
    assertFalse(response.isSuccess());
    assertNull(response.getBody());
    // Error body should be null when no error stream
    assertNull(response.getErrorBody());
  }

  @Test
  void testMultilineResponse() throws IOException {
    server.createContext(
        "/multiline",
        exchange -> {
          String response = "line1\nline2\nline3";
          sendResponse(exchange, 200, response);
        });

    Map<String, String> headers = new HashMap<>();
    HttpClient.HttpResponse response = client.get(getBaseUrl() + "/multiline", headers);

    assertEquals(200, response.getStatusCode());
    assertTrue(response.getBody().contains("line1"));
    assertTrue(response.getBody().contains("line2"));
    assertTrue(response.getBody().contains("line3"));
  }

  @Test
  void testInformationalResponse() throws IOException {
    // Test status code < 200 (informational responses)
    // This covers the missing branch in readResponse where statusCode < 200
    server.createContext(
        "/informational",
        exchange -> {
          // Status 199 exercises the branch where statusCode < 200
          // Note: HttpServer forces contentLen=-1 for status < 200
          exchange.sendResponseHeaders(199, -1);
          exchange.close();
        });

    Map<String, String> headers = new HashMap<>();
    HttpClient.HttpResponse response = client.get(getBaseUrl() + "/informational", headers);

    assertEquals(199, response.getStatusCode());
    assertFalse(response.isSuccess());
    assertNull(response.getBody());
    // Error body is null because HttpServer doesn't send body for status < 200
    assertNull(response.getErrorBody());
  }

  // ==================== Helper Methods ====================

  private String readRequestBody(HttpExchange exchange) throws IOException {
    try (InputStream is = exchange.getRequestBody()) {
      StringBuilder sb = new StringBuilder();
      byte[] buffer = new byte[1024];
      int bytesRead;
      while ((bytesRead = is.read(buffer)) != -1) {
        sb.append(new String(buffer, 0, bytesRead, StandardCharsets.UTF_8));
      }
      return sb.toString();
    }
  }

  private void sendResponse(HttpExchange exchange, int statusCode, String response)
      throws IOException {
    byte[] responseBytes = response.getBytes(StandardCharsets.UTF_8);
    exchange.sendResponseHeaders(statusCode, responseBytes.length);
    try (OutputStream os = exchange.getResponseBody()) {
      os.write(responseBytes);
    }
  }
}
