# Contributing to Zerobus SDK for Java

We happily welcome contributions to the Zerobus SDK for Java. We use [GitHub Issues](https://github.com/databricks/zerobus-sdk-java/issues) to track community reported issues and [GitHub Pull Requests](https://github.com/databricks/zerobus-sdk-java/pulls) for accepting changes.

Contributions are licensed on a license-in/license-out basis.

## Communication

Before starting work on a major feature, please open a GitHub issue. We will make sure no one else is already working on it and that it is aligned with the goals of the project.

A "major feature" is defined as any change that is > 100 LOC altered (not including tests), or changes any user-facing behavior.

We will use the GitHub issue to discuss the feature and come to agreement. This is to prevent your time being wasted, as well as ours. The GitHub review process for major features is also important so that organizations with commit access can come to agreement on design.

If it is appropriate to write a design document, the document must be hosted either in the GitHub tracking issue, or linked to from the issue and hosted in a world-readable location.

Small patches and bug fixes don't need prior communication.

## Development Setup

### Prerequisites

- **Java**: 8 or higher - [Download Java](https://adoptium.net/)
- **Maven**: 3.6 or higher - [Download Maven](https://maven.apache.org/download.cgi)
- **Protocol Buffers Compiler** (`protoc`): 24.4 - [Download protoc](https://github.com/protocolbuffers/protobuf/releases/tag/v24.4)
- Git

### Setting Up Your Development Environment

1. **Clone the repository:**
   ```bash
   git clone https://github.com/databricks/zerobus-sdk-java.git
   cd zerobus-sdk-java
   ```

2. **Build the project:**
   ```bash
   mvn clean install
   ```

   This will:
   - Generate protobuf Java classes
   - Compile the source code
   - Run tests
   - Install the artifact to your local Maven repository

3. **Run tests:**
   ```bash
   mvn test
   ```

## Coding Style

Code style is enforced by [Spotless](https://github.com/diffplug/spotless) in your pull request. We use Google Java Format for code formatting.

### Running the Formatter

Format your code before committing:

```bash
mvn spotless:apply
```

This will format:
- **Java code**: Using Google Java Format
- **Imports**: Organized and unused imports removed
- **pom.xml**: Sorted dependencies and plugins

### Checking Formatting

Check if your code is properly formatted:

```bash
mvn spotless:check
```

## Pull Request Process

1. **Create a feature branch:**
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. **Make your changes:**
   - Write clear, concise commit messages
   - Follow existing code style
   - Add tests for new functionality
   - Update documentation as needed

3. **Format your code:**
   ```bash
   mvn spotless:apply
   ```

4. **Run tests:**
   ```bash
   mvn test
   ```

5. **Commit your changes:**
   ```bash
   git add .
   git commit -s -m "Add feature: description of your changes"
   ```

   Note: The `-s` flag signs your commit (required - see below).

6. **Push to your fork:**
   ```bash
   git push origin feature/your-feature-name
   ```

7. **Create a Pull Request:**
   - Provide a clear description of changes
   - Reference any related issues
   - Ensure all CI checks pass

## Signed Commits

This repo requires all contributors to sign their commits. To configure this, you can follow [Github's documentation](https://docs.github.com/en/authentication/managing-commit-signature-verification/signing-commits) to create a GPG key, upload it to your Github account, and configure your git client to sign commits.

## Developer Certificate of Origin

To contribute to this repository, you must sign off your commits to certify that you have the right to contribute the code and that it complies with the open source license. The rules are pretty simple, if you can certify the content of [DCO](./DCO), then simply add a "Signed-off-by" line to your commit message to certify your compliance. Please use your real name as pseudonymous/anonymous contributions are not accepted.

```
Signed-off-by: Joe Smith <joe.smith@email.com>
```

If you set your `user.name` and `user.email` git configs, you can sign your commit automatically with `git commit -s`:

```bash
git commit -s -m "Your commit message"
```

## Code Review Guidelines

When reviewing code:

- Check for adherence to code style (Google Java Format)
- Look for potential edge cases
- Consider performance implications
- Ensure documentation is updated
- Verify tests cover new functionality

## Commit Message Guidelines

Follow these conventions for commit messages:

- Use present tense: "Add feature" not "Added feature"
- Use imperative mood: "Fix bug" not "Fixes bug"
- First line should be 50 characters or less
- Reference issues: "Fix #123: Description of fix"

Example:
```
Add async stream creation example

- Add BlockingIngestionExample.java demonstrating synchronous ingestion
- Update README with blocking API documentation

Fixes #42
```

## Documentation

### Updating Documentation

- Add Javadoc for all public APIs
- Use standard Javadoc format
- Include `@param`, `@return`, `@throws` tags
- Update README.md for user-facing changes
- Update examples/ for new features

Example Javadoc:
```java
/**
 * Ingests a single record into the stream.
 *
 * <p>Returns a CompletableFuture that completes when the record is durably written to storage.
 * This method may block if the maximum number of in-flight records has been reached.
 *
 * @param record The protobuf message to ingest
 * @return A CompletableFuture that completes when the record is acknowledged
 * @throws ZerobusException if the stream is not in a valid state for ingestion
 */
public CompletableFuture<Void> ingestRecord(RecordType record) throws ZerobusException {
    // ...
}
```

## Testing

### Writing Tests

- Add unit tests for all new functionality
- Use JUnit 5 for test framework
- Use Mockito for mocking
- Tests should be fast (< 1 second per test)
- Use descriptive test names

Example test:
```java
@Test
public void testSingleRecordIngestAndAcknowledgment() throws Exception {
    // Given
    ZerobusStream<TestMessage> stream = createTestStream();

    // When
    CompletableFuture<Void> result = stream.ingestRecord(testMessage);

    // Then
    result.get(5, TimeUnit.SECONDS);
    assertEquals(StreamState.OPENED, stream.getState());
}
```

### Running Tests

```bash
# Run all tests
mvn test

# Run specific test class
mvn test -Dtest=ZerobusSdkTest

# Run specific test method
mvn test -Dtest=ZerobusSdkTest#testSingleRecordIngestAndAcknowledgment
```

## Continuous Integration

All pull requests must pass CI checks:

- **Build**: `mvn clean compile`
- **Tests**: `mvn test` (on Java 11, 17, 21)
- **Formatting**: `mvn spotless:check`

Tests run on both Ubuntu and Windows runners.

You can view CI results in the GitHub Actions tab of the pull request.

## Maven Commands

Useful Maven commands:

```bash
# Clean build
mvn clean

# Compile code
mvn compile

# Run tests
mvn test

# Format code
mvn spotless:apply

# Check formatting
mvn spotless:check

# Create JARs (regular + fat JAR)
mvn package

# Install to local Maven repo
mvn install

# Generate protobuf classes
mvn protobuf:compile
```

## Versioning

We follow [Semantic Versioning](https://semver.org/):

- **MAJOR**: Incompatible API changes
- **MINOR**: Backwards-compatible functionality additions
- **PATCH**: Backwards-compatible bug fixes

## Getting Help

- **Issues**: Open an issue on GitHub for bugs or feature requests
- **Discussions**: Use GitHub Discussions for questions
- **Documentation**: Check the README and examples/

## Package Name

The package is published on Maven Central as:
- **Group ID**: `com.databricks`
- **Artifact ID**: `zerobus-ingest-sdk`

## Code of Conduct

- Be respectful and inclusive
- Welcome newcomers
- Focus on constructive feedback
- Follow professional open source etiquette
