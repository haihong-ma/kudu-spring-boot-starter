package com.kad.cube.kudu.exceptions;

/**
 * @author haihong.ma
 */
public class EntityPrimaryKeyNotNullException extends CubeKuduException {
    public EntityPrimaryKeyNotNullException(String message) {
        super(message);
    }

    public EntityPrimaryKeyNotNullException(String message, Throwable cause) {
        super(message, cause);
    }

    public EntityPrimaryKeyNotNullException(Throwable cause) {
        super(cause);
    }
}
