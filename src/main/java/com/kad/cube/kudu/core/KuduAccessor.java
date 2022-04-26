package com.kad.cube.kudu.core;

import com.kad.cube.kudu.connection.KuduSessionFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * @author haihong.ma
 */
public class KuduAccessor implements InitializingBean {
    protected final Log logger = LogFactory.getLog(this.getClass());
    @Nullable
    private KuduSessionFactory kuduSessionFactory;

    public KuduAccessor() {
    }

    public void afterPropertiesSet() {
        Assert.state(this.getKuduSessionFactory() != null, "KuduSessionFactory is required");
    }

    @Nullable
    public KuduSessionFactory getKuduSessionFactory() {
        return this.kuduSessionFactory;
    }

    public KuduSessionFactory getRequiredKuduSessionFactory() {
        KuduSessionFactory kuduSessionFactory = this.getKuduSessionFactory();
        if (kuduSessionFactory == null) {
            throw new IllegalStateException("KuduSessionFactory is required");
        } else {
            return kuduSessionFactory;
        }
    }

    public void setKuduSessionFactory(KuduSessionFactory kuduSessionFactory) {
        this.kuduSessionFactory = kuduSessionFactory;
    }
}
