package com.kad.cube.kudu.connection;

import com.kad.cube.kudu.config.KuduProperties;
import com.kad.cube.kudu.config.KuduSessionPoolConfig;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduSession;

/**
 * @author haihong.ma
 */
public class KuduSessionPool extends Pool<KuduSession> {

    public KuduSessionPool(KuduClient kuduClient, KuduSessionPoolConfig poolConfig, KuduProperties.Session session) {
        super(poolConfig, new PooledKuduSessionFactory(kuduClient, session));
    }
}