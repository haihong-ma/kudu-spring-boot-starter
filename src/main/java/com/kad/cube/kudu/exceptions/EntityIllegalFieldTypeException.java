package com.kad.cube.kudu.exceptions;

/**
 * @author haihong.ma
 */
public class EntityIllegalFieldTypeException extends CubeKuduException {
    public EntityIllegalFieldTypeException(String message) {
        super(message);
    }

    public EntityIllegalFieldTypeException(String message, Throwable cause) {
        super(message, cause);
    }

    public EntityIllegalFieldTypeException(Throwable cause) {
        super(cause);
    }
}