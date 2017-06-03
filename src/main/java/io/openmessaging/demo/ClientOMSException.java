package io.openmessaging.demo;

import io.openmessaging.exception.OMSRuntimeException;

/**
 * Created by lee on 5/16/17.
 */
public class ClientOMSException extends OMSRuntimeException {
    public String message;
    public ClientOMSException(String message) {
        this.message = message;
    }
    public ClientOMSException(String message, Throwable throwable) {
        this.initCause(throwable);
        this.message = message;
    }

    @Override
    public String getMessage() {
        return message;
    }
}
