package com.databricks.zerobus.stream;

import io.grpc.Status;
import java.util.HashSet;
import java.util.Set;

/** Utility for classifying gRPC errors as retriable or non-retriable. */
public class GrpcErrorHandling {
  private static final Set<Status.Code> NON_RETRIABLE_CODES = new HashSet<>();

  static {
    NON_RETRIABLE_CODES.add(Status.Code.INVALID_ARGUMENT);
    NON_RETRIABLE_CODES.add(Status.Code.NOT_FOUND);
    NON_RETRIABLE_CODES.add(Status.Code.UNAUTHENTICATED);
    NON_RETRIABLE_CODES.add(Status.Code.OUT_OF_RANGE);
  }

  public static boolean isNonRetriable(Status.Code code) {
    return NON_RETRIABLE_CODES.contains(code);
  }
}
