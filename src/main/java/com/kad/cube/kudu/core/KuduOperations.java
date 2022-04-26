package com.kad.cube.kudu.core;

import org.apache.kudu.Schema;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduPredicate;
import org.apache.kudu.client.OperationResponse;

import java.util.List;
import java.util.function.Function;

/**
 * @author haihong.ma
 */
public interface KuduOperations {
    <T> OperationResponse insert(T objectToInsert) throws KuduException;

    <T> OperationResponse upsert(T objectToUpsert) throws KuduException;

    <T> OperationResponse update(T objectToUpdate) throws KuduException;

    <T> OperationResponse delete(T objectToDelete) throws KuduException;

    <T> void insert(List<T> objectsToInsert) throws KuduException;

    <T> void upsert(List<T> objectsToUpsert) throws KuduException;

    <T> void update(List<T> objectsToUpdate) throws KuduException;

    <T> void delete(List<T> objectsToDelete) throws KuduException;

    <T> List<T> query(Function<Schema, List<KuduPredicate>> predicateBuilder, Class<T> targetClass) throws KuduException;
}

