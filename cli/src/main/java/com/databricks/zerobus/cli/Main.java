package com.databricks.zerobus.cli;

/**
 * Main entry point for the Zerobus CLI.
 *
 * <p>Usage:
 *
 * <pre>{@code
 * # Generate proto schema from Unity Catalog table
 * java -jar zerobus-cli-0.1.0.jar generate-proto \
 *   --uc-endpoint "https://your-workspace.cloud.databricks.com" \
 *   --client-id "your-client-id" \
 *   --client-secret "your-client-secret" \
 *   --table "catalog.schema.table" \
 *   --output "output.proto"
 * }</pre>
 */
public class Main {

  private static final String VERSION = "0.1.0";

  private static final String USAGE =
      "Zerobus CLI - Tools for Databricks Zerobus\n"
          + "\n"
          + "Usage: java -jar zerobus-cli.jar <command> [options]\n"
          + "\n"
          + "Commands:\n"
          + "  generate-proto    Generate proto2 schema from Unity Catalog table\n"
          + "  version           Show version information\n"
          + "  help              Show this help message\n"
          + "\n"
          + "Examples:\n"
          + "  java -jar zerobus-cli.jar generate-proto \\\n"
          + "    --uc-endpoint \"https://your-workspace.cloud.databricks.com\" \\\n"
          + "    --client-id \"your-client-id\" \\\n"
          + "    --client-secret \"your-client-secret\" \\\n"
          + "    --table \"catalog.schema.table\" \\\n"
          + "    --output \"output.proto\"\n"
          + "\n"
          + "For command-specific help:\n"
          + "  java -jar zerobus-cli.jar generate-proto --help\n";

  public static void main(String[] args) {
    if (args.length == 0) {
      System.out.println(USAGE);
      System.exit(0);
    }

    String command = args[0];

    switch (command) {
      case "generate-proto":
        String[] protoArgs = new String[args.length - 1];
        System.arraycopy(args, 1, protoArgs, 0, args.length - 1);
        GenerateProto.main(protoArgs);
        break;

      case "version":
      case "--version":
      case "-v":
        System.out.println("zerobus-cli " + VERSION);
        break;

      case "help":
      case "--help":
      case "-h":
        System.out.println(USAGE);
        break;

      default:
        System.err.println("Unknown command: " + command);
        System.err.println();
        System.err.println(USAGE);
        System.exit(1);
    }
  }
}
