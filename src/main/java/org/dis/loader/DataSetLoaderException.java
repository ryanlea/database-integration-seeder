package org.dis.loader;

/**
 * <p>
 * Used for fatal errors when loading data sets fails.
 * </p>
 */
class DataSetLoaderException extends RuntimeException {

    DataSetLoaderException(String message, Throwable cause) {
        super(message, cause);
    }
}
