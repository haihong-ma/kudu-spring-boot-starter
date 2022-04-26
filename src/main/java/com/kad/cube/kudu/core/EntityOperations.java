package com.kad.cube.kudu.core;

import com.kad.cube.kudu.core.annotation.PrimaryKey;
import com.kad.cube.kudu.core.annotation.Table;
import com.kad.cube.kudu.exceptions.EntityAnnotationNotFoundException;
import org.springframework.util.StringUtils;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author haihong.ma
 */
public class EntityOperations {
    private static final Map<Class<?>, EntityInfo> ENTITY_INFO_MAP = new ConcurrentHashMap<>();

    public EntityOperations() {
    }

    public static EntityOperations.EntityInfo getEntityInfo(Class<?> clazz) {
        if (!ENTITY_INFO_MAP.containsKey(clazz)) {
            doGetEntityInfo(clazz);
        }

        return ENTITY_INFO_MAP.get(clazz);
    }

    private static void doGetEntityInfo(Class<?> clazz) {
        if (!clazz.isAnnotationPresent(Table.class)) {
            throw new EntityAnnotationNotFoundException("Please annotation the table name");
        }
        EntityOperations.EntityInfo entityInfo = new EntityOperations.EntityInfo();
        Table tableAnnotation = clazz.getAnnotation(Table.class);
        entityInfo.setTableName(tableAnnotation.value());
        if (!StringUtils.hasLength(entityInfo.getTableName())) {
            entityInfo.setTableName(clazz.getName());
        }

        for (Field field : clazz.getDeclaredFields()) {
            String fieldName = null;
            boolean isPrimaryKey = false;
            if (field.isAnnotationPresent(PrimaryKey.class)) {
                fieldName = field.getAnnotation(PrimaryKey.class).value();
                isPrimaryKey = true;
            } else if (field.isAnnotationPresent(com.kad.cube.kudu.core.annotation.Field.class)) {
                fieldName = field.getAnnotation(com.kad.cube.kudu.core.annotation.Field.class).value();
            }

            if (!StringUtils.hasLength(fieldName)) {
                fieldName = field.getName();
            }

            entityInfo.fieldMap.put(fieldName, new EntityOperations.FieldInfo(field, isPrimaryKey));
        }

        ENTITY_INFO_MAP.put(clazz, entityInfo);
    }

    public static class FieldInfo {
        private final Field field;
        private final boolean isPrimaryKey;

        public FieldInfo(Field field, boolean isPrimaryKey) {
            if (!field.isAccessible()) {
                field.setAccessible(true);
            }

            this.field = field;
            this.isPrimaryKey = isPrimaryKey;
        }

        public Field getField() {
            return this.field;
        }

        public boolean isPrimaryKey() {
            return this.isPrimaryKey;
        }
    }

    public static class EntityInfo {
        private String tableName;
        private boolean isVerified;
        private final Map<String, EntityOperations.FieldInfo> fieldMap = new HashMap<>();

        public EntityInfo() {
        }

        public String getTableName() {
            return this.tableName;
        }

        public void setTableName(String tableName) {
            this.tableName = tableName;
        }

        public boolean isVerified() {
            return this.isVerified;
        }

        public void setVerified(boolean verified) {
            this.isVerified = verified;
        }

        public Map<String, EntityOperations.FieldInfo> getFieldMap() {
            return this.fieldMap;
        }
    }
}
