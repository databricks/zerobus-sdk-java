package com.databricks.zerobus.common.http;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import javax.annotation.Nonnull;

/**
 * Default implementation of {@link HttpClient} using {@link HttpURLConnection}.
 *
 * <p>This is the production implementation used by the SDK.
 *
 * <p>Use the singleton instance via {@link #INSTANCE}.
 */
public class DefaultHttpClient implements HttpClient {

  public static final DefaultHttpClient INSTANCE = new DefaultHttpClient();

  private DefaultHttpClient() {}

  @Override
  @Nonnull
  public HttpResponse post(
      @Nonnull String url, @Nonnull String formData, @Nonnull Map<String, String> headers)
      throws IOException {
    HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
    try {
      connection.setRequestMethod("POST");
      connection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
      connection.setDoOutput(true);

      for (Map.Entry<String, String> entry : headers.entrySet()) {
        connection.setRequestProperty(entry.getKey(), entry.getValue());
      }

      try (OutputStreamWriter writer =
          new OutputStreamWriter(connection.getOutputStream(), StandardCharsets.UTF_8)) {
        writer.write(formData);
      }

      return readResponse(connection);
    } finally {
      connection.disconnect();
    }
  }

  @Override
  @Nonnull
  public HttpResponse get(@Nonnull String url, @Nonnull Map<String, String> headers)
      throws IOException {
    HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
    try {
      connection.setRequestMethod("GET");

      for (Map.Entry<String, String> entry : headers.entrySet()) {
        connection.setRequestProperty(entry.getKey(), entry.getValue());
      }

      return readResponse(connection);
    } finally {
      connection.disconnect();
    }
  }

  private HttpResponse readResponse(HttpURLConnection connection) throws IOException {
    int statusCode = connection.getResponseCode();

    String body = null;
    String errorBody = null;

    if (statusCode >= 200 && statusCode < 300) {
      body = readStream(connection.getInputStream());
    } else {
      errorBody = readStream(connection.getErrorStream());
    }

    return new HttpResponse(statusCode, body, errorBody);
  }

  private String readStream(InputStream stream) throws IOException {
    if (stream == null) {
      return null;
    }
    try (BufferedReader reader =
        new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
      StringBuilder builder = new StringBuilder();
      String line;
      while ((line = reader.readLine()) != null) {
        builder.append(line).append("\n");
      }
      return builder.toString();
    }
  }
}
