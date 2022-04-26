package com.kad.cube.kudu.connection;

import com.kad.cube.kudu.exceptions.CubeKuduException;
import com.kad.cube.kudu.exceptions.KuduConnectionException;
import com.kad.cube.kudu.exceptions.KuduExhaustedPoolException;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import java.io.Closeable;
import java.util.NoSuchElementException;

/**
 * @author haihong.ma
 */
public abstract class Pool<T> implements Closeable {
    protected GenericObjectPool<T> internalPool;

    public Pool(final GenericObjectPoolConfig<T> poolConfig, PooledObjectFactory<T> factory) {
        this.initPool(poolConfig, factory);
    }

    public void close() {
        this.destroy();
    }

    public boolean isClosed() {
        return this.internalPool.isClosed();
    }

    public void initPool(final GenericObjectPoolConfig<T> poolConfig, PooledObjectFactory<T> factory) {
        if (this.internalPool != null) {
            this.closeInternalPool();
        }

        this.internalPool = new GenericObjectPool<>(factory, poolConfig);
    }

    public T getResource() {
        try {
            return this.internalPool.borrowObject();
        } catch (NoSuchElementException ex) {
            if (null == ex.getCause()) {
                throw new KuduExhaustedPoolException("Could not get a resource since the pool is exhausted", ex);
            } else {
                throw new CubeKuduException("Could not get a resource from the pool", ex);
            }
        } catch (Exception ex) {
            throw new KuduConnectionException("Could not get a resource from the pool", ex);
        }
    }

    public void returnResourceObject(final T resource) {
        if (null != resource) {
            try {
                this.internalPool.returnObject(resource);
            } catch (Exception ex) {
                throw new CubeKuduException("Could not return the resource to the pool", ex);
            }
        }
    }

    protected void returnBrokenResource(final T resource) {
        if (resource != null) {
            this.returnBrokenResourceObject(resource);
        }

    }

    protected void returnResource(final T resource) {
        if (resource != null) {
            this.returnResourceObject(resource);
        }

    }

    private void destroy() {
        this.closeInternalPool();
    }

    protected void returnBrokenResourceObject(final T resource) {
        try {
            this.internalPool.invalidateObject(resource);
        } catch (Exception ex) {
            throw new CubeKuduException("Could not return the broken resource to the pool", ex);
        }
    }

    protected void closeInternalPool() {
        try {
            this.internalPool.close();
        } catch (Exception ex) {
            throw new CubeKuduException("Could not destroy the pool", ex);
        }
    }

    public int getNumActive() {
        return this.poolInactive() ? -1 : this.internalPool.getNumActive();
    }

    public int getNumIdle() {
        return this.poolInactive() ? -1 : this.internalPool.getNumIdle();
    }

    public int getNumWaiters() {
        return this.poolInactive() ? -1 : this.internalPool.getNumWaiters();
    }

    public long getMeanBorrowWaitTimeMillis() {
        return this.poolInactive() ? -1L : this.internalPool.getMeanBorrowWaitTimeMillis();
    }

    public long getMaxBorrowWaitTimeMillis() {
        return this.poolInactive() ? -1L : this.internalPool.getMaxBorrowWaitTimeMillis();
    }

    private boolean poolInactive() {
        return this.internalPool == null || this.internalPool.isClosed();
    }

    public void addObjects(int count) {
        try {
            for (int i = 0; i < count; ++i) {
                this.internalPool.addObject();
            }

        } catch (Exception ex) {
            throw new CubeKuduException("Error trying to add idle objects", ex);
        }
    }
}
