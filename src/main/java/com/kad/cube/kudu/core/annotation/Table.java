package com.kad.cube.kudu.core.annotation;

import java.lang.annotation.*;

/**
 * @author haihong.ma
 */
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface Table {
    String value() default "";
}

