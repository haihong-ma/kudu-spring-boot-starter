package com.kad.cube.kudu.exceptions;

/**
 * @author haihong.ma
 */
public class KuduConnectionException extends CubeKuduException {
    public KuduConnectionException(String message) {
        super(message);
    }

    public KuduConnectionException(String message, Throwable cause) {
        super(message, cause);
    }

    public KuduConnectionException(Throwable cause) {
        super(cause);
    }
}
