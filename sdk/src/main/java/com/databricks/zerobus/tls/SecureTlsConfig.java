package com.databricks.zerobus.tls;

import io.grpc.ChannelCredentials;
import io.grpc.TlsChannelCredentials;

/**
 * Secure TLS configuration using system CA certificates.
 *
 * <p>This is the default configuration, enabling TLS encryption using the operating system's
 * trusted CA certificates. It provides secure communication without requiring additional
 * configuration.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * // Explicit usage (functionally identical to default)
 * TlsConfig tls = new SecureTlsConfig();
 * ZerobusStream<MyRecord> stream = sdk.createStream(
 *     tableProperties,
 *     clientId,
 *     clientSecret,
 *     options,
 *     null,  // headersProvider
 *     tls    // tlsConfig
 * ).join();
 *
 * // Default usage (SecureTlsConfig is used automatically)
 * ZerobusStream<MyRecord> stream = sdk.createStream(
 *     tableProperties,
 *     clientId,
 *     clientSecret,
 *     options
 * ).join();
 * }</pre>
 *
 * @see TlsConfig
 * @see com.databricks.zerobus.ZerobusSdk#createStream
 */
public class SecureTlsConfig extends TlsConfig {

  /**
   * Returns secure TLS credentials using system CA certificates.
   *
   * @return SSL channel credentials with system CAs
   */
  @Override
  public ChannelCredentials toChannelCredentials() {
    return TlsChannelCredentials.create();
  }
}
