package com.kad.cube.kudu.config;

import com.kad.cube.kudu.connection.KuduSessionFactory;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduSession;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author haihong.ma
 */
@Configuration
@EnableConfigurationProperties({KuduProperties.class})
@ConditionalOnClass({GenericObjectPool.class, KuduClient.class, KuduSession.class})
public class KuduSessionConfiguration {
    private final KuduProperties properties;

    public KuduSessionConfiguration(KuduProperties properties) {
        this.properties = properties;
    }

    @Bean
    @ConditionalOnMissingBean({KuduSessionFactory.class})
    public KuduSessionFactory kuduSessionFactory(KuduClient kuduClient) {
        KuduSessionFactory factory = new KuduSessionFactory(kuduClient);
        KuduProperties.Pool pool = this.properties.getPool();
        if (pool != null) {
            this.applyPooling(pool, factory);
        }

        factory.setSession(this.properties.getSession());
        return factory;
    }

    @Bean
    @ConditionalOnMissingBean(
            name = {"kuduClient"}
    )
    public KuduClient kuduClient(KuduProperties kuduProperties) {
        KuduClient.KuduClientBuilder builder = new KuduClient.KuduClientBuilder(kuduProperties.getMasterAddresses());
        if (kuduProperties.getDefaultAdminOperationTimeoutMs() != null) {
            builder.defaultAdminOperationTimeoutMs(kuduProperties.getDefaultOperationTimeoutMs());
        }

        if (kuduProperties.getDefaultOperationTimeoutMs() != null) {
            builder.defaultOperationTimeoutMs(kuduProperties.getDefaultOperationTimeoutMs());
        }

        if (kuduProperties.getDefaultSocketReadTimeoutMs() != null) {
            builder.defaultSocketReadTimeoutMs(kuduProperties.getDefaultSocketReadTimeoutMs());
        }

        if (kuduProperties.isDisableStatistics()) {
            builder.disableStatistics();
        }

        if (kuduProperties.getWorkerCount() != null) {
            builder.workerCount(kuduProperties.getWorkerCount());
        }

        if (kuduProperties.getBossCount() != null) {
            builder.bossCount(kuduProperties.getBossCount());
        }

        return builder.build();
    }

    private void applyPooling(KuduProperties.Pool pool, KuduSessionFactory factory) {
        factory.setKuduSessionPoolConfig(this.kuduSessionPoolConfig(pool));
    }

    private KuduSessionPoolConfig kuduSessionPoolConfig(KuduProperties.Pool pool) {
        KuduSessionPoolConfig config = new KuduSessionPoolConfig();
        config.setMaxTotal(pool.getMaxActive());
        config.setMaxIdle(pool.getMaxIdle());
        config.setMinIdle(pool.getMinIdle());
        if (pool.getMaxWait() != null) {
            config.setMaxWaitMillis(pool.getMaxWait().toMillis());
        }
        return config;
    }
}
