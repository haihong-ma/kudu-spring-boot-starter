package com.kad.cube.kudu.config;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.kudu.client.KuduSession;

/**
 * @author haihong.ma
 */
public class KuduSessionPoolConfig extends GenericObjectPoolConfig<KuduSession> {
    public KuduSessionPoolConfig() {
        this.setTestWhileIdle(true);
        this.setMinEvictableIdleTimeMillis(60000L);
        this.setTimeBetweenEvictionRunsMillis(30000L);
        this.setNumTestsPerEvictionRun(-1);
    }
}
