package com.databricks.zerobus;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import javax.annotation.Nonnull;

/**
 * Builder for creating {@link ZerobusSdk} instances with custom configuration.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * ZerobusSdk sdk = ZerobusSdk.builder(endpoint, ucEndpoint)
 *     .executor(myExecutor)
 *     .build();
 * }</pre>
 *
 * @see ZerobusSdk#builder(String, String)
 */
public final class ZerobusSdkBuilder {
  private final String serverEndpoint;
  private final String unityCatalogEndpoint;
  private Optional<ExecutorService> executor = Optional.empty();
  private Optional<ZerobusSdkStubFactory> stubFactory = Optional.empty();

  /**
   * Creates a new ZerobusSdkBuilder.
   *
   * <p>Use {@link ZerobusSdk#builder(String, String)} instead of calling this constructor directly.
   *
   * @param serverEndpoint The gRPC endpoint URL for the Zerobus service
   * @param unityCatalogEndpoint The Unity Catalog endpoint URL
   */
  ZerobusSdkBuilder(@Nonnull String serverEndpoint, @Nonnull String unityCatalogEndpoint) {
    this.serverEndpoint = Objects.requireNonNull(serverEndpoint, "serverEndpoint cannot be null");
    this.unityCatalogEndpoint =
        Objects.requireNonNull(unityCatalogEndpoint, "unityCatalogEndpoint cannot be null");
  }

  /**
   * Sets a custom executor service for the SDK.
   *
   * <p>If not set, the SDK will create a cached thread pool that automatically scales based on
   * demand. When providing a custom executor, the caller is responsible for shutting it down.
   *
   * @param executor The executor service to use
   * @return This builder for method chaining
   */
  @Nonnull
  public ZerobusSdkBuilder executor(@Nonnull ExecutorService executor) {
    this.executor = Optional.of(Objects.requireNonNull(executor, "executor cannot be null"));
    return this;
  }

  /**
   * Sets a custom stub factory for the SDK.
   *
   * <p>This is primarily used for testing.
   *
   * @param stubFactory The stub factory to use
   * @return This builder for method chaining
   */
  @Nonnull
  ZerobusSdkBuilder stubFactory(@Nonnull ZerobusSdkStubFactory stubFactory) {
    this.stubFactory =
        Optional.of(Objects.requireNonNull(stubFactory, "stubFactory cannot be null"));
    return this;
  }

  /**
   * Builds the ZerobusSdk instance.
   *
   * @return A new ZerobusSdk instance
   */
  @Nonnull
  public ZerobusSdk build() {
    return new ZerobusSdk(
        serverEndpoint,
        unityCatalogEndpoint,
        executor.orElseGet(ZerobusSdk::createDefaultExecutor),
        stubFactory.orElseGet(ZerobusSdkStubFactory::new));
  }
}
