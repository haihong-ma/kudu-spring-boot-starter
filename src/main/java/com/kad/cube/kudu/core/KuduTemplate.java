package com.kad.cube.kudu.core;

import com.kad.cube.kudu.connection.KuduSessionFactory;
import com.kad.cube.kudu.core.converter.KuduConverter;
import com.kad.cube.kudu.core.enums.OperationType;
import org.apache.kudu.Schema;
import org.apache.kudu.client.*;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * @author haihong.ma
 */
public class KuduTemplate extends KuduAccessor implements KuduOperations {
    private final KuduConverter kuduConverter;

    public KuduTemplate(KuduConverter kuduConverter) {
        this.kuduConverter = kuduConverter;
    }

    public <T> OperationResponse insert(T objectToInsert) throws KuduException {
        return this.doOperation(objectToInsert, OperationType.INSERT);
    }

    public <T> OperationResponse upsert(T objectToUpsert) throws KuduException {
        return this.doOperation(objectToUpsert, OperationType.UPSERT);
    }

    public <T> OperationResponse update(T objectToUpdate) throws KuduException {
        return this.doOperation(objectToUpdate, OperationType.UPDATE);
    }

    public <T> OperationResponse delete(T objectToDelete) throws KuduException {
        return this.doOperation(objectToDelete, OperationType.DELETE);
    }

    public <T> void insert(List<T> objectsToInsert) throws KuduException {
        this.doOperation(objectsToInsert, OperationType.INSERT);
    }

    public <T> void upsert(List<T> objectsToUpsert) throws KuduException {
        this.doOperation(objectsToUpsert, OperationType.UPSERT);
    }

    public <T> void update(List<T> objectsToUpdate) throws KuduException {
        this.doOperation(objectsToUpdate, OperationType.UPDATE);
    }

    public <T> void delete(List<T> objectsToDelete) throws KuduException {
        this.doOperation(objectsToDelete, OperationType.DELETE);
    }

    public <T> List<T> query(Function<Schema, List<KuduPredicate>> predicateBuilder, Class<T> targetClass) throws KuduException {
        Assert.notNull(predicateBuilder, "predicateBuilder must not be null!");
        Assert.notNull(targetClass, "targetClass must not be null!");
        KuduSessionFactory sessionFactory = this.getRequiredKuduSessionFactory();
        EntityOperations.EntityInfo entityInfo = EntityOperations.getEntityInfo(targetClass);
        KuduTable kuduTable = sessionFactory.openTable(entityInfo.getTableName());
        KuduClient kuduClient = sessionFactory.getKuduClient();
        KuduScanner.KuduScannerBuilder scannerBuilder = kuduClient.newScannerBuilder(kuduTable);
        List<KuduPredicate> kuduPredicates = predicateBuilder.apply(kuduTable.getSchema());
        for (KuduPredicate predicate : kuduPredicates) {
            scannerBuilder.addPredicate(predicate);
        }
        return this.kuduConverter.mapToResult(scannerBuilder.build(), kuduTable, targetClass, entityInfo);
    }

    private <T> OperationResponse doOperation(T objectToOperate, OperationType operationType) throws KuduException {
        Assert.notNull(objectToOperate, "ObjectToOperate must not be null!");
        EntityOperations.EntityInfo entityInfo = EntityOperations.getEntityInfo(objectToOperate.getClass());
        KuduSessionFactory kuduSessionFactory = this.getRequiredKuduSessionFactory();
        KuduTable table = kuduSessionFactory.openTable(entityInfo.getTableName());
        Operation operation = this.kuduConverter.convert(objectToOperate, table, entityInfo, operationType);
        KuduSession kuduSession = kuduSessionFactory.getKuduSession();

        OperationResponse response;
        try {
            response = kuduSession.apply(operation);
        } finally {
            kuduSessionFactory.returnKuduSession(kuduSession);
        }
        return response;
    }

    private <T> void doOperation(List<T> objectsToOperate, OperationType operationType) throws KuduException {
        Assert.notEmpty(objectsToOperate, "ObjectToOperate must not be null or empty!");
        EntityOperations.EntityInfo entityInfo = EntityOperations.getEntityInfo(objectsToOperate.get(0).getClass());
        KuduSessionFactory kuduSessionFactory = this.getRequiredKuduSessionFactory();
        KuduTable table = kuduSessionFactory.openTable(entityInfo.getTableName());
        List<Operation> operations = new ArrayList<>(objectsToOperate.size());
        objectsToOperate.forEach(operate -> operations.add(this.kuduConverter.convert(operate, table, entityInfo, operationType)));

        boolean isThrowException = false;
        KuduSession kuduSession = kuduSessionFactory.getKuduSession();

        try {
            kuduSession.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
            int realCount = operations.size();
            int maxManualFlushCount = kuduSessionFactory.getMaxManualFlushCount();
            int loopCount = realCount / maxManualFlushCount + (realCount % maxManualFlushCount == 0 ? 0 : 1);

            try {
                for (int loopNum = 0; loopNum < loopCount; ++loopNum) {
                    int estimateEndNum = (loopNum + 1) * maxManualFlushCount;
                    int realEndNum = Math.min(estimateEndNum, realCount);

                    for (int startNum = loopNum * maxManualFlushCount; startNum < realEndNum; ++startNum) {
                        kuduSession.apply(operations.get(startNum));
                    }

                    kuduSession.flush();
                }
            } catch (Exception ex) {
                isThrowException = true;
                throw ex;
            }
        } finally {
            if (isThrowException) {
                kuduSession.flush();
            }
            kuduSession.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC);
            kuduSessionFactory.returnKuduSession(kuduSession);
        }

    }
}
