package com.kad.cube.kudu.core.annotation;

import java.lang.annotation.*;

/**
 * @author haihong.ma
 */
@Target({ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface PrimaryKey {
    String value() default "";
}
