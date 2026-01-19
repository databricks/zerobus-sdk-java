package com.databricks.zerobus.tls;

import io.grpc.ChannelCredentials;

/**
 * Abstract base class for TLS configuration strategies.
 *
 * <p>Implementations define how to configure the gRPC channel's TLS settings. By default, the SDK
 * uses secure TLS with system CA certificates. Custom implementations can provide alternative TLS
 * configurations such as custom certificate authorities or mutual TLS.
 *
 * <p>Example usage with custom TLS configuration:
 *
 * <pre>{@code
 * public class CustomTlsConfig extends TlsConfig {
 *     private final File caCertFile;
 *
 *     public CustomTlsConfig(File caCertFile) {
 *         this.caCertFile = caCertFile;
 *     }
 *
 *     @Override
 *     public ChannelCredentials toChannelCredentials() {
 *         try {
 *             return TlsChannelCredentials.newBuilder()
 *                 .trustManager(caCertFile)
 *                 .build();
 *         } catch (IOException e) {
 *             throw new RuntimeException("Failed to load CA certificate", e);
 *         }
 *     }
 * }
 *
 * TlsConfig customTls = new CustomTlsConfig(new File("/path/to/ca-cert.pem"));
 * ZerobusStream<MyRecord> stream = sdk.createStream(
 *     tableProperties,
 *     clientId,
 *     clientSecret,
 *     options,
 *     customTls
 * ).join();
 * }</pre>
 *
 * <p>For most use cases, the default TLS configuration is sufficient and no custom implementation
 * is needed. Simply call {@code createStream} without TLS parameters:
 *
 * <pre>{@code
 * ZerobusStream<MyRecord> stream = sdk.createStream(
 *     tableProperties,
 *     clientId,
 *     clientSecret,
 *     options
 * ).join();
 * }</pre>
 *
 * @see com.databricks.zerobus.ZerobusSdk#createStream
 */
public abstract class TlsConfig {

  /**
   * Converts TLS configuration to gRPC ChannelCredentials.
   *
   * @return Channel credentials for secure connection
   */
  public abstract ChannelCredentials toChannelCredentials();
}
