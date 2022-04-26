package com.kad.cube.kudu.connection;

import com.kad.cube.kudu.config.KuduProperties;
import com.kad.cube.kudu.exceptions.CubeKuduException;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduSession;

/**
 * @author haihong.ma
 */
public class PooledKuduSessionFactory implements PooledObjectFactory<KuduSession> {
    private final KuduClient kuduClient;
    private final KuduProperties.Session session;

    public PooledKuduSessionFactory(KuduClient kuduClient, KuduProperties.Session session) {
        this.session = session;
        this.kuduClient = kuduClient;
    }

    public PooledObject<KuduSession> makeObject() {
        KuduSession kuduSession = this.kuduClient.newSession();
        if (this.session != null && this.session.getMaxManualFlushCount() > 0) {
            kuduSession.setMutationBufferSpace(this.session.getMaxManualFlushCount());
        }

        return new DefaultPooledObject<>(kuduSession);
    }

    public void destroyObject(PooledObject<KuduSession> pooledObject) {
        KuduSession kuduSession = pooledObject.getObject();
        if (!kuduSession.isClosed()) {
            try {
                kuduSession.close();
            }catch (Exception ex){
                throw new CubeKuduException(ex);
            }
        }

    }

    public boolean validateObject(PooledObject<KuduSession> pooledObject) {
        KuduSession kuduSession = pooledObject.getObject();
        return !kuduSession.isClosed();
    }

    public void activateObject(PooledObject<KuduSession> pooledObject) {
    }

    public void passivateObject(PooledObject<KuduSession> pooledObject) {
    }
}
