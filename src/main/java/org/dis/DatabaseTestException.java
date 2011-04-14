package org.dis;

public class DatabaseTestException extends RuntimeException {
    public DatabaseTestException(String message) {
        super(message);
    }

    public DatabaseTestException(String message, Throwable cause) {
        super(message, cause);
    }
}
