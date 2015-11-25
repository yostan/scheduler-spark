package com.gz.dt.lock;

import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by naonao on 2015/10/27.
 */
public class MemoryLocks {

    final private HashMap<String, ReentrantReadWriteLock> locks = new HashMap<String, ReentrantReadWriteLock>();

    private static enum Type {
        READ, WRITE
    }

    class MemoryLockToken implements LockToken {
        private final ReentrantReadWriteLock rwLock;
        private final java.util.concurrent.locks.Lock lock;
        private final String resource;

        private MemoryLockToken(ReentrantReadWriteLock rwLock, java.util.concurrent.locks.Lock lock, String resource) {
            this.rwLock = rwLock;
            this.lock = lock;
            this.resource = resource;
        }

        public void release() {
            int val = rwLock.getQueueLength();
            if (val == 0) {
                synchronized (locks) {
                    locks.remove(resource);
                }
            }
            lock.unlock();
        }
    }


    public int size() {
        return locks.size();
    }


    public MemoryLockToken getReadLock(String resource, long wait) throws InterruptedException {
        return getLock(resource, Type.READ, wait);
    }

    public MemoryLockToken getWriteLock(String resource, long wait) throws InterruptedException {
        return getLock(resource, Type.WRITE, wait);
    }


    private MemoryLockToken getLock(String resource, Type type, long wait) throws InterruptedException {
        ReentrantReadWriteLock lockEntry;
        synchronized (locks) {
            if (locks.containsKey(resource)) {
                lockEntry = locks.get(resource);
            }
            else {
                lockEntry = new ReentrantReadWriteLock(true);
                locks.put(resource, lockEntry);
            }
        }

        Lock lock = (type.equals(Type.READ)) ? lockEntry.readLock() : lockEntry.writeLock();

        if (wait == -1) {
            lock.lock();
        }
        else {
            if (wait > 0) {
                if (!lock.tryLock(wait, TimeUnit.MILLISECONDS)) {
                    return null;
                }
            }
            else {
                if (!lock.tryLock()) {
                    return null;
                }
            }
        }
        synchronized (locks) {
            if (!locks.containsKey(resource)) {
                locks.put(resource, lockEntry);
            }
        }
        return new MemoryLockToken(lockEntry, lock, resource);
    }

}
