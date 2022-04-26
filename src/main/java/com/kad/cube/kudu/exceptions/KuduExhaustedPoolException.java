package com.kad.cube.kudu.exceptions;

/**
 * @author haihong.ma
 */
public class KuduExhaustedPoolException extends CubeKuduException {
    public KuduExhaustedPoolException(String message) {
        super(message);
    }

    public KuduExhaustedPoolException(String message, Throwable cause) {
        super(message, cause);
    }

    public KuduExhaustedPoolException(Throwable cause) {
        super(cause);
    }
}
