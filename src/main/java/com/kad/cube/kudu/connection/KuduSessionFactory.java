package com.kad.cube.kudu.connection;

import com.kad.cube.kudu.config.KuduProperties;
import com.kad.cube.kudu.config.KuduSessionPoolConfig;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author haihong.ma
 */
public class KuduSessionFactory {
    private final KuduClient kuduClient;
    private KuduProperties.Session session;
    private KuduSessionPoolConfig poolConfig;
    private static volatile KuduSessionPool kuduSessionPool;
    private static final Map<String, KuduTable> KUDU_TABLE_MAP = new ConcurrentHashMap<>();

    public KuduSessionFactory(KuduClient kuduClient) {
        this.kuduClient = kuduClient;
    }

    public void setKuduSessionPoolConfig(KuduSessionPoolConfig poolConfig) {
        this.poolConfig = poolConfig;
    }

    public void setSession(KuduProperties.Session session) {
        this.session = session;
    }

    public int getMaxManualFlushCount() {
        return this.session == null ? (new KuduProperties.Session()).getMaxManualFlushCount() : this.session.getMaxManualFlushCount();
    }

    private KuduSessionPool getKuduSessionPool() {
        if (kuduSessionPool == null) {
            synchronized(this) {
                if (kuduSessionPool == null) {
                    kuduSessionPool = new KuduSessionPool(this.kuduClient, this.poolConfig, this.session);
                }
            }
        }

        return kuduSessionPool;
    }

    public KuduTable openTable(String tableName) throws KuduException {
        if (!KUDU_TABLE_MAP.containsKey(tableName)) {
            KUDU_TABLE_MAP.put(tableName, this.kuduClient.openTable(tableName));
        }

        return KUDU_TABLE_MAP.get(tableName);
    }

    public KuduClient getKuduClient() {
        return this.kuduClient;
    }

    public KuduSession getKuduSession() {
        if (this.poolConfig == null) {
            KuduSession kuduSession = this.kuduClient.newSession();
            if (this.session != null && this.session.getMaxManualFlushCount() > 0) {
                kuduSession.setMutationBufferSpace(this.session.getMaxManualFlushCount());
            }
            return this.kuduClient.newSession();
        }
        return this.getKuduSessionPool().getResource();
    }

    public void returnKuduSession(KuduSession kuduSession) {
        KuduSessionPool kuduSessionPool = this.getKuduSessionPool();
        if (kuduSessionPool != null) {
            kuduSessionPool.returnResourceObject(kuduSession);
        }
    }
}
