package com.kad.cube.kudu.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.util.StringUtils;

import java.time.Duration;

/**
 * @author haihong.ma
 */
@ConfigurationProperties("cube.kudu")
public class KuduProperties {
    private static final String DEFAULT_MASTER_ADDRESSES = "localhost";
    private String masterAddresses;
    private Long defaultAdminOperationTimeoutMs;
    private Long defaultOperationTimeoutMs;
    private Long defaultSocketReadTimeoutMs;
    private boolean disableStatistics;
    private Integer bossCount;
    private Integer workerCount;
    private KuduProperties.Pool pool;
    private KuduProperties.Session session;

    public KuduProperties() {
    }

    public String getMasterAddresses() {
        return StringUtils.hasLength(this.masterAddresses) ? this.masterAddresses : DEFAULT_MASTER_ADDRESSES;
    }

    public void setMasterAddresses(String masterAddresses) {
        this.masterAddresses = masterAddresses;
    }

    public Long getDefaultAdminOperationTimeoutMs() {
        return this.defaultAdminOperationTimeoutMs;
    }

    public void setDefaultAdminOperationTimeoutMs(Long defaultAdminOperationTimeoutMs) {
        this.defaultAdminOperationTimeoutMs = defaultAdminOperationTimeoutMs;
    }

    public Long getDefaultOperationTimeoutMs() {
        return this.defaultOperationTimeoutMs;
    }

    public void setDefaultOperationTimeoutMs(Long defaultOperationTimeoutMs) {
        this.defaultOperationTimeoutMs = defaultOperationTimeoutMs;
    }

    public Long getDefaultSocketReadTimeoutMs() {
        return this.defaultSocketReadTimeoutMs;
    }

    public void setDefaultSocketReadTimeoutMs(Long defaultSocketReadTimeoutMs) {
        this.defaultSocketReadTimeoutMs = defaultSocketReadTimeoutMs;
    }

    public boolean isDisableStatistics() {
        return this.disableStatistics;
    }

    public void setDisableStatistics(boolean disableStatistics) {
        this.disableStatistics = disableStatistics;
    }

    public Integer getBossCount() {
        return this.bossCount;
    }

    public void setBossCount(Integer bossCount) {
        this.bossCount = bossCount;
    }

    public Integer getWorkerCount() {
        return this.workerCount;
    }

    public void setWorkerCount(Integer workerCount) {
        this.workerCount = workerCount;
    }

    public KuduProperties.Pool getPool() {
        return this.pool;
    }

    public void setPool(KuduProperties.Pool pool) {
        this.pool = pool;
    }

    public KuduProperties.Session getSession() {
        return this.session;
    }

    public void setSession(KuduProperties.Session session) {
        this.session = session;
    }

    public static class Session {
        private int maxManualFlushCount = 1000;

        public Session() {
        }

        public int getMaxManualFlushCount() {
            return this.maxManualFlushCount;
        }

        public void setMaxManualFlushCount(int maxManualFlushCount) {
            this.maxManualFlushCount = maxManualFlushCount;
        }
    }

    public static class Pool {
        private int maxIdle = 8;
        private int minIdle = 0;
        private int maxActive = 8;
        private Duration maxWait = Duration.ofMillis(-1L);

        public Pool() {
        }

        public int getMaxIdle() {
            return this.maxIdle;
        }

        public void setMaxIdle(int maxIdle) {
            this.maxIdle = maxIdle;
        }

        public int getMinIdle() {
            return this.minIdle;
        }

        public void setMinIdle(int minIdle) {
            this.minIdle = minIdle;
        }

        public int getMaxActive() {
            return this.maxActive;
        }

        public void setMaxActive(int maxActive) {
            this.maxActive = maxActive;
        }

        public Duration getMaxWait() {
            return this.maxWait;
        }

        public void setMaxWait(Duration maxWait) {
            this.maxWait = maxWait;
        }
    }
}
