package com.gz.dt.util;

import java.util.Iterator;
import java.util.concurrent.TimeUnit;

/**
 * Created by naonao on 2015/10/27.
 */
public class PollablePriorityDelayQueue<E> extends PriorityDelayQueue<E> {

    public PollablePriorityDelayQueue(int priorities, long maxWait, TimeUnit unit, int maxSize) {
        super(priorities, maxWait, unit, maxSize);
    }

    @Override
    public QueueElement<E> poll() {
        lock.lock();
        try {
            antiStarvation();
            QueueElement<E> e = null;
            int i = priorities;
            for (; e == null && i > 0; i--) {
                e = queues[i - 1].peek();
                if (eligibleToPoll(e)) {
                    e = queues[i - 1].poll();
                }
                else {
                    if (e != null) {
                        debug("poll(): the peek element [{0}], from P[{1}] is not eligible to poll", e.getElement().toString(), i);
                    }
                    e = null;
                    Iterator<QueueElement<E>> iter = queues[i - 1].iterator();
                    while(e == null && iter.hasNext()) {
                        e = iter.next();
                        if (e.getDelay(TimeUnit.MILLISECONDS) <= 0 && eligibleToPoll(e)) {
                            queues[i - 1].remove(e);
                        }
                        else {
                            debug("poll(): the iterator element [{0}], from P[{1}] is not eligible to poll", e.getElement().toString(), i);
                            e = null;
                        }
                    }
                }
            }
            if (e != null) {
                if (currentSize != null) {
                    currentSize.decrementAndGet();
                }
                e.inQueue = false;
                debug("poll(): [{0}], from P[{1}]", e.getElement().toString(), i);
            }
            return e;
        }
        finally {
            lock.unlock();
        }
    }

    protected boolean eligibleToPoll(QueueElement<?> element) {
        return true;
    }
}
