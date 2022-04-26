package com.kad.cube.kudu.exceptions;

/**
 * @author haihong.ma
 */
public class CubeKuduException extends RuntimeException {
    private static final long serialVersionUID = -2946266495682282677L;

    public CubeKuduException(String message) {
        super(message);
    }

    public CubeKuduException(String message, Throwable cause) {
        super(message, cause);
    }

    public CubeKuduException(Throwable cause) {
        super(cause);
    }
}
