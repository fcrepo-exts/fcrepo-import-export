package org.fcrepo.importexport.common;


public class BinaryImportException extends RuntimeException {

    public BinaryImportException(String message) {
        super(message);
    }

    public BinaryImportException(Throwable cause) {
        super(cause);
    }

    public BinaryImportException(String message, Throwable cause) {
        super(message, cause);
    }

    public BinaryImportException(String message, Throwable cause, boolean enableSuppression,
            boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

}
