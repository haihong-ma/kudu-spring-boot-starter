package com.kad.cube.kudu.core.converter;

import com.kad.cube.kudu.core.EntityOperations;
import com.kad.cube.kudu.core.enums.OperationType;
import com.kad.cube.kudu.exceptions.CubeKuduException;
import com.kad.cube.kudu.exceptions.EntityIllegalFieldTypeException;
import com.kad.cube.kudu.exceptions.EntityPrimaryKeyNotNullException;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;
import org.springframework.util.SerializationUtils;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.*;

/**
 * @author haihong.ma
 */
public class DefaultKuduConverter implements KuduConverter {

    public <T> Operation convert(T objectToOperate, KuduTable table, EntityOperations.EntityInfo entityInfo, OperationType operationType) {
        List<ColumnSchema> columnSchemas = table.getSchema().getColumns();
        Map<String, EntityOperations.FieldInfo> fieldInfoMap = entityInfo.getFieldMap();
        if (!entityInfo.isVerified()) {
            this.doVerify(columnSchemas, fieldInfoMap);
            entityInfo.setVerified(true);
        }

        return this.doGetOperation(objectToOperate, table, columnSchemas, fieldInfoMap, operationType);
    }

    public <T> List<T> mapToResult(KuduScanner scanner, KuduTable table, Class<T> targetClass, EntityOperations.EntityInfo entityInfo) {
        List<ColumnSchema> columnSchemas = table.getSchema().getColumns();
        Map<String, EntityOperations.FieldInfo> fieldInfoMap = entityInfo.getFieldMap();
        if (!entityInfo.isVerified()) {
            this.doVerify(columnSchemas, fieldInfoMap);
            entityInfo.setVerified(true);
        }

        return this.doGetResult(scanner, fieldInfoMap, targetClass, columnSchemas);
    }

    private <T> Operation doGetOperation(T objectToOperate, KuduTable table, List<ColumnSchema> columnSchemas, Map<String, EntityOperations.FieldInfo> fieldInfoMap, OperationType operationType) {
        Operation operation = this.createOperation(operationType, table);
        PartialRow row = operation.getRow();
        columnSchemas.forEach(columnSchema -> {
            String columnName = columnSchema.getName();
            if (fieldInfoMap.containsKey(columnName)) {
                EntityOperations.FieldInfo fieldInfo = fieldInfoMap.get(columnName);
                if (operationType != OperationType.DELETE || fieldInfo.isPrimaryKey()) {
                    Field field = fieldInfo.getField();
                    Type type = columnSchema.getType();
                    Object fieldValue = this.doGetValue(objectToOperate, field);
                    if (Objects.isNull(fieldValue)) {
                        if (fieldInfo.isPrimaryKey()) {
                            throw new EntityPrimaryKeyNotNullException("Primary key '" + field.getName() + "'must not be null or empty");
                        }
                    } else {
                        switch (type) {
                            case STRING:
                                row.addString(columnName, typeCasting(field, fieldValue, columnName, String.class));
                                break;
                            case INT64:
                                row.addLong(columnName, typeCasting(field, fieldValue, columnName, Long.class));
                                break;
                            case UNIXTIME_MICROS:
                                Timestamp timestamp;
                                if (field.getType() == LocalDateTime.class) {
                                    timestamp = Timestamp.valueOf(((LocalDateTime) fieldValue));
                                } else if (field.getType() == Date.class) {
                                    timestamp = new Timestamp(((Date) fieldValue).getTime());
                                } else {
                                    throw new CubeKuduException("date type [" + field.getType().getName() + "nonsupport");
                                }
                                row.addTimestamp(columnName, timestamp);
                                break;
                            case DOUBLE:
                                row.addDouble(columnName, typeCasting(field, fieldValue, columnName, Double.class));
                                break;
                            case INT32:
                                row.addInt(columnName, typeCasting(field, fieldValue, columnName, Integer.class));
                                break;
                            case INT16:
                                row.addShort(columnName, typeCasting(field, fieldValue, columnName, Short.class));
                                break;
                            case INT8:
                                row.addByte(columnName, typeCasting(field, fieldValue, columnName, Byte.class));
                                break;
                            case BOOL:
                                row.addBoolean(columnName, typeCasting(field, fieldValue, columnName, Boolean.class));
                                break;
                            case BINARY:
                                row.addBinary(columnName, SerializationUtils.serialize(fieldValue));
                                break;
                            case FLOAT:
                                row.addFloat(columnName, typeCasting(field, fieldValue, columnName, Float.class));
                                break;
                            case DECIMAL:
                                row.addDecimal(columnName, typeCasting(field, fieldValue, columnName, BigDecimal.class));
                        }
                    }
                }
            }
        });
        return operation;
    }

    private <T> Object doGetValue(T objectToOperate, Field field) {
        try {
            return field.get(objectToOperate);
        } catch (IllegalAccessException var4) {
            throw new CubeKuduException("Can't access field '" + field.getName() + "'");
        }
    }

    private void doVerify(List<ColumnSchema> columnSchemas, Map<String, EntityOperations.FieldInfo> fieldInfoMap) {
        columnSchemas.forEach(columnSchema -> {
            String columnName = columnSchema.getName();
            if (fieldInfoMap.containsKey(columnName)) {
                EntityOperations.FieldInfo fieldInfo = fieldInfoMap.get(columnName);
                if (columnSchema.isKey() && !fieldInfo.isPrimaryKey()) {
                    throw new CubeKuduException("Column '" + columnName + "' is primary key,must have @PrimaryKey annotation");
                }

                Field field = fieldInfo.getField();
                Type type = columnSchema.getType();
                switch (type) {
                    case STRING:
                        this.verifyFieldType(columnName, Collections.singletonList(String.class), field.getType());
                        break;
                    case INT64:
                        this.verifyFieldType(columnName, Arrays.asList(Long.class, Long.TYPE), field.getType());
                        break;
                    case UNIXTIME_MICROS:
                        this.verifyFieldType(columnName, Arrays.asList(Date.class, LocalDateTime.class), field.getType());
                        break;
                    case DOUBLE:
                        this.verifyFieldType(columnName, Arrays.asList(Double.class, Double.TYPE), field.getType());
                        break;
                    case INT32:
                        this.verifyFieldType(columnName, Arrays.asList(Integer.class, Integer.TYPE), field.getType());
                        break;
                    case INT16:
                        this.verifyFieldType(columnName, Arrays.asList(Short.class, Short.TYPE), field.getType());
                        break;
                    case INT8:
                        this.verifyFieldType(columnName, Arrays.asList(Byte.class, Byte.TYPE), field.getType());
                        break;
                    case BOOL:
                        this.verifyFieldType(columnName, Arrays.asList(Boolean.class, Boolean.TYPE), field.getType());
                    case BINARY:
                    default:
                        break;
                    case FLOAT:
                        this.verifyFieldType(columnName, Arrays.asList(Float.class, Float.TYPE), field.getType());
                        break;
                    case DECIMAL:
                        this.verifyFieldType(columnName, Collections.singletonList(BigDecimal.class), field.getType());
                }
            }
        });
    }

    private void verifyFieldType(String columnName, List<java.lang.reflect.Type> supportTypes, java.lang.reflect.Type realType) {
        if (!supportTypes.contains(realType)) {
            throw new EntityIllegalFieldTypeException("Field '" + columnName + "' must be '" + supportTypes + "' type");
        }
    }

    private Operation createOperation(OperationType operationType, KuduTable table) {
        switch (operationType) {
            case INSERT:
                return table.newInsert();
            case UPDATE:
                return table.newUpdate();
            case UPSERT:
                return table.newUpsert();
            case DELETE:
                return table.newDelete();
            default:
                throw new CubeKuduException("Un support operation type " + operationType);
        }
    }

    private <T> List<T> doGetResult(KuduScanner scanner, Map<String, EntityOperations.FieldInfo> fieldInfoMap, Class<T> targetClass, List<ColumnSchema> columnSchemas) {
        try {
            List<T> targetList = new ArrayList<>();

            while (scanner.hasMoreRows()) {
                RowResultIterator iterator = scanner.nextRows();

                while (iterator.hasNext()) {
                    RowResult rowResult = iterator.next();
                    T target = targetClass.newInstance();
                    for (ColumnSchema columnSchema : columnSchemas) {
                        String columnName = columnSchema.getName();
                        if (fieldInfoMap.containsKey(columnName)) {
                            Field field = fieldInfoMap.get(columnName).getField();
                            this.doSetValue(target, columnSchema.getType(), rowResult, columnName, field);
                        }
                    }
                    targetList.add(target);
                }
            }
            return targetList;
        } catch (Exception ex) {
            throw new CubeKuduException(ex);
        }
    }

    private <T> void doSetValue(T target, Type type, RowResult rowResult, String columnName, Field field) throws IllegalAccessException {
        if (!rowResult.isNull(columnName)) {
            switch (type) {
                case STRING:
                    field.set(target, rowResult.getString(columnName));
                    break;
                case INT64:
                    field.set(target, rowResult.getLong(columnName));
                    break;
                case UNIXTIME_MICROS:
                    Timestamp timestamp = rowResult.getTimestamp(columnName);
                    if (field.getType() == LocalDateTime.class) {
                        field.set(target, timestamp.toLocalDateTime());
                    } else {
                        field.set(target, new Date(timestamp.getTime()));
                    }
                    break;
                case DOUBLE:
                    field.set(target, rowResult.getDouble(columnName));
                    break;
                case INT32:
                    field.set(target, rowResult.getInt(columnName));
                    break;
                case INT16:
                    field.set(target, rowResult.getShort(columnName));
                    break;
                case INT8:
                    field.set(target, rowResult.getByte(columnName));
                    break;
                case BOOL:
                    field.set(target, rowResult.getBoolean(columnName));
                    break;
                case BINARY:
                    field.set(target, rowResult.getBinary(columnName));
                    break;
                case FLOAT:
                    field.set(target, rowResult.getFloat(columnName));
                    break;
                case DECIMAL:
                    field.set(target, rowResult.getDecimal(columnName));
            }
        }
    }

    @SuppressWarnings("unchecked")
    private <T> T typeCasting(Field field, Object fieldValue, String columnName, Class<T> columnClass) {
        if (!columnClass.isAssignableFrom(field.getType())) {
            throw new CubeKuduException(String.format("field [%s] type [%s] not matched column [%s] type [%s]",
                    field.getName(), fieldValue.getClass().getName(), columnName, columnClass.getName()));
        }
        return (T) fieldValue;
    }
}
