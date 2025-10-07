package com.databricks.zerobus;

import java.util.logging.Logger;
import java.util.logging.Level;

/**
 * Base logging class for the Zerobus SDK.
 * Uses java.util.logging for simple console logging.
 */
class ZerobusSdkLogging {
    protected final Logger logger;

    protected ZerobusSdkLogging() {
        this.logger = Logger.getLogger(this.getClass().getName());
    }

    protected void debug(String message) {
        logger.log(Level.FINE, message);
    }

    protected void info(String message) {
        logger.log(Level.INFO, message);
    }

    protected void warn(String message) {
        logger.log(Level.WARNING, message);
    }

    protected void error(String message) {
        logger.log(Level.SEVERE, message);
    }

    protected void error(String message, Throwable t) {
        logger.log(Level.SEVERE, message, t);
    }
}
