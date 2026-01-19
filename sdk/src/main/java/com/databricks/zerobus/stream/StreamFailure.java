package com.databricks.zerobus.stream;

/** Types of stream failures that can occur during ingestion. */
enum StreamFailureType {
  UNKNOWN,
  SERVER_CLOSED_STREAM,
  SENDING_MESSAGE,
  SERVER_UNRESPONSIVE
}

/** Tracks stream failure counts and types for recovery decisions. */
class StreamFailureInfo {
  private StreamFailureType failureType = StreamFailureType.UNKNOWN;
  private int failureCounts = 0;

  synchronized void logFailure(StreamFailureType type) {
    if (type == failureType) {
      failureCounts++;
    } else {
      failureType = type;
      failureCounts = 1;
    }
  }

  synchronized void resetFailure(StreamFailureType type) {
    if (failureType == type) {
      failureCounts = 0;
      failureType = StreamFailureType.UNKNOWN;
    }
  }

  synchronized int getFailureCounts() {
    return failureCounts;
  }
}
