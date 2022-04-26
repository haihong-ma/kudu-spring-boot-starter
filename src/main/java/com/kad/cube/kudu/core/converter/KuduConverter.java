package com.kad.cube.kudu.core.converter;

import com.kad.cube.kudu.core.EntityOperations;
import com.kad.cube.kudu.core.enums.OperationType;
import org.apache.kudu.client.KuduScanner;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.Operation;

import java.util.List;

/**
 * @author haihong.ma
 */
public interface KuduConverter {

    <T> Operation convert(T objectToOperate, KuduTable table, EntityOperations.EntityInfo entityInfo, OperationType operationType);

    <T> List<T> mapToResult(KuduScanner scanner, KuduTable table, Class<T> targetClass, EntityOperations.EntityInfo entityInfo);
}
