# NEXT CHANGELOG

## Release v0.2.0

### New Features and Improvements

- Updated Protocol Buffers from 3.24.0 to 4.33.0 for improved performance and latest features
- Updated gRPC dependencies from 1.58.0 to 1.76.0 for enhanced stability and security
- Updated SLF4J logging framework from 1.7.36 to 2.0.17 for modern logging capabilities

### Bug Fixes

### Documentation

- Updated README.md with new dependency versions
- Updated protoc compiler version recommendations
- Updated Logback version compatibility for SLF4J 2.0

### Internal Changes

- Updated maven-compiler-plugin from 3.11.0 to 3.14.1
- All gRPC artifacts now consistently use version 1.76.0

### API Changes

**⚠️ Breaking Changes:**

- **Protocol Buffers 4.x Migration**: If you use the regular JAR (not the fat JAR), you must upgrade to protobuf-java 4.33.0 and regenerate any custom `.proto` files using protoc 4.x
  - Download protoc 4.33.0 from: https://github.com/protocolbuffers/protobuf/releases/tag/v33.0
  - Regenerate proto files: `protoc --java_out=src/main/java src/main/proto/record.proto`
  - Protobuf 4.x is binary-compatible over the wire with 3.x, but generated Java code may differ

- **SLF4J 2.0 Migration**: If you use a logging implementation, you may need to update it:
  - `slf4j-simple`: Use version 2.0.17 or later
  - `logback-classic`: Use version 1.4.14 or later (for SLF4J 2.0 compatibility)
  - `log4j-slf4j-impl`: Use version 2.20.0 or later

**Note**: If you use the fat JAR (`jar-with-dependencies`), all dependencies are bundled and no action is required.
