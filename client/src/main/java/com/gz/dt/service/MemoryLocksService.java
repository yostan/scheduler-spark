package com.gz.dt.service;

import com.gz.dt.exception.ServiceException;
import com.gz.dt.lock.LockToken;
import com.gz.dt.lock.MemoryLocks;
import com.gz.dt.util.Instrumentable;
import com.gz.dt.util.Instrumentation;

/**
 * Created by naonao on 2015/10/27.
 */
public class MemoryLocksService implements Service, Instrumentable {

    protected static final String INSTRUMENTATION_GROUP = "locks";
    private MemoryLocks locks;

    public LockToken getReadLock(String resource, long wait) throws InterruptedException {
        return locks.getReadLock(resource, wait);
    }

    public LockToken getWriteLock(String resource, long wait) throws InterruptedException {
        return locks.getWriteLock(resource, wait);
    }


    public void instrument(Instrumentation instr) {
        final MemoryLocks finalLocks = this.locks;
        instr.addVariable(INSTRUMENTATION_GROUP, "locks", new Instrumentation.Variable<Long>() {
            public Long getValue() {
                return (long) finalLocks.size();
            }
        });
    }

    public void init(Services services) throws ServiceException {
        locks = new MemoryLocks();
    }

    public void destroy() {
        locks = null;
    }

    public Class<? extends Service> getInterface() {
        return MemoryLocksService.class;
    }
}
