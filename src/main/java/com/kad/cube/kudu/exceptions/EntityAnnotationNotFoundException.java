package com.kad.cube.kudu.exceptions;

/**
 * @author haihong.ma
 */
public class EntityAnnotationNotFoundException extends CubeKuduException {
    public EntityAnnotationNotFoundException(String message) {
        super(message);
    }

    public EntityAnnotationNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }

    public EntityAnnotationNotFoundException(Throwable cause) {
        super(cause);
    }
}

