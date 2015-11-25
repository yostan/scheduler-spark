package com.gz.dt.service;

import com.gz.dt.client.SubmitClient;
import com.gz.dt.command.XCallable;
import com.gz.dt.exception.ServiceException;
import com.gz.dt.util.*;
import org.apache.hadoop.conf.Configuration;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by naonao on 2015/10/27.
 */
public class CallableQueueService implements Service, Instrumentable {

    private static final String INSTRUMENTATION_GROUP = "callablequeue";
    private static final String INSTR_IN_QUEUE_TIME_TIMER = "time.in.queue";
    private static final String INSTR_EXECUTED_COUNTER = "executed";
    private static final String INSTR_FAILED_COUNTER = "failed";
    private static final String INSTR_QUEUED_COUNTER = "queued";
    private static final String INSTR_QUEUE_SIZE_SAMPLER = "queue.size";
    private static final String INSTR_THREADS_ACTIVE_SAMPLER = "threads.active";

    public static final String CONF_PREFIX = Service.CONF_PREFIX + "CallableQueueService.";

    public static final String CONF_QUEUE_SIZE = CONF_PREFIX + "queue.size";
    public static final String CONF_THREADS = CONF_PREFIX + "threads";
    public static final String CONF_CALLABLE_CONCURRENCY = CONF_PREFIX + "callable.concurrency";
    public static final String CONF_CALLABLE_NEXT_ELIGIBLE = CONF_PREFIX + "callable.next.eligible";
    public static final String CONF_CALLABLE_INTERRUPT_TYPES = CONF_PREFIX + "InterruptTypes";
    public static final String CONF_CALLABLE_INTERRUPT_MAP_MAX_SIZE = CONF_PREFIX + "InterruptMapMaxSize";

    public static final int CONCURRENCY_DELAY = 500;

    public static final int SAFE_MODE_DELAY = 60000;

    private final Map<String, AtomicInteger> activeCallables = new HashMap<String, AtomicInteger>();

    private final Map<String, Date> uniqueCallables = new ConcurrentHashMap<String, Date>();

    private final ConcurrentHashMap<String, Set<XCallable<?>>> interruptCommandsMap = new ConcurrentHashMap<String, Set<XCallable<?>>>();

    public static final HashSet<String> INTERRUPT_TYPES = new HashSet<String>();

    private int interruptMapMaxSize;

    private int maxCallableConcurrency;


    private boolean callableBegin(XCallable<?> callable) {
        synchronized (activeCallables) {
            AtomicInteger counter = activeCallables.get(callable.getType());
            if (counter == null) {
                counter = new AtomicInteger(1);
                activeCallables.put(callable.getType(), counter);
                return true;
            }
            else {
                int i = counter.incrementAndGet();
                return i <= maxCallableConcurrency;
            }
        }
    }


    private void callableEnd(XCallable<?> callable) {
        synchronized (activeCallables) {
            AtomicInteger counter = activeCallables.get(callable.getType());
            if (counter == null) {
                throw new IllegalStateException("It should not happen");
            }
            else {
                counter.decrementAndGet();
            }
        }
    }

    private boolean callableReachMaxConcurrency(XCallable<?> callable) {
        synchronized (activeCallables) {
            AtomicInteger counter = activeCallables.get(callable.getType());
            if (counter == null) {
                return true;
            }
            else {
                int i = counter.get();
                return i < maxCallableConcurrency;
            }
        }
    }


    private XLog log = XLog.getLog(getClass());

    private int queueSize;
    private PriorityDelayQueue<CallableWrapper> queue;
    private ThreadPoolExecutor executor;
    private Instrumentation instrumentation;

    private void incrCounter(String name, int count) {
        if (instrumentation != null) {
            instrumentation.incr(INSTRUMENTATION_GROUP, name, count);
        }
    }

    private void addInQueueCron(Instrumentation.Cron cron) {
        if (instrumentation != null) {
            instrumentation.addCron(INSTRUMENTATION_GROUP, INSTR_IN_QUEUE_TIME_TIMER, cron);
        }
    }










    public void instrument(Instrumentation instr) {
        instrumentation = instr;
        instr.addSampler(INSTRUMENTATION_GROUP, INSTR_QUEUE_SIZE_SAMPLER, 60, 1, new Instrumentation.Variable<Long>() {
            public Long getValue() {
                return (long) queue.size();
            }
        });
        instr.addSampler(INSTRUMENTATION_GROUP, INSTR_THREADS_ACTIVE_SAMPLER, 60, 1,
                new Instrumentation.Variable<Long>() {
                    public Long getValue() {
                        return (long) executor.getActiveCount();
                    }
                });
    }

    public void init(Services services) throws ServiceException {
        Configuration conf = services.getConf();

        queueSize = ConfigurationService.getInt(conf, CONF_QUEUE_SIZE);  //default 10000
        int threads = ConfigurationService.getInt(conf, CONF_THREADS);   //default 10
        boolean callableNextEligible = ConfigurationService.getBoolean(conf, CONF_CALLABLE_NEXT_ELIGIBLE);  //default true

        for (String type : ConfigurationService.getStrings(conf, CONF_CALLABLE_INTERRUPT_TYPES)) {
            log.debug("Adding interrupt type [{0}]", type);
            INTERRUPT_TYPES.add(type);
        }

        if (!callableNextEligible) {
            queue = new PriorityDelayQueue<CallableWrapper>(3, 1000 * 30, TimeUnit.MILLISECONDS, queueSize) {
                @Override
                protected void debug(String msgTemplate, Object... msgArgs) {
                    log.trace(msgTemplate, msgArgs);
                }
            };
        }
        else {
            // If the head of this queue has already reached max concurrency,
            // continuously find next one
            // which has not yet reach max concurrency.Overrided method
            // 'eligibleToPoll' to check if the
            // element of this queue has reached the maximum concurrency.
            queue = new PollablePriorityDelayQueue<CallableWrapper>(3, 1000 * 30, TimeUnit.MILLISECONDS, queueSize) {
                @Override
                protected void debug(String msgTemplate, Object... msgArgs) {
                    log.trace(msgTemplate, msgArgs);
                }

                @Override
                protected boolean eligibleToPoll(QueueElement<?> element) {
                    if (element != null) {
                        CallableWrapper wrapper = (CallableWrapper) element;
                        if (element.getElement() != null) {
                            return callableReachMaxConcurrency(wrapper.getElement());
                        }
                    }
                    return false;
                }

            };
        }

        interruptMapMaxSize = ConfigurationService.getInt(conf, CONF_CALLABLE_INTERRUPT_MAP_MAX_SIZE); //default 500

        // IMPORTANT: The ThreadPoolExecutor does not always the execute
        // commands out of the queue, there are
        // certain conditions where commands are pushed directly to a thread.
        // As we are using a queue with DELAYED semantics (i.e. execute the
        // command in 5 mins) we need to make
        // sure that the commands are always pushed to the queue.
        // To achieve this (by looking a the ThreadPoolExecutor.execute()
        // implementation, we are making the pool
        // minimum size equals to the maximum size (thus threads are keep always
        // running) and we are warming up
        // all those threads (the for loop that runs dummy runnables).
        executor = new ThreadPoolExecutor(threads, threads, 10, TimeUnit.SECONDS, (BlockingQueue) queue){
            protected void beforeExecute(Thread t, Runnable r) {
                super.beforeExecute(t,r);
                XLog.Info.get().clear();
            }
        };

        for (int i = 0; i < threads; i++) {
            executor.execute(new Runnable() {
                public void run() {
                    try {
                        Thread.sleep(100);
                    }
                    catch (InterruptedException ex) {
                        log.warn("Could not warm up threadpool {0}", ex.getMessage(), ex);
                    }
                }
            });
        }

        maxCallableConcurrency = ConfigurationService.getInt(conf, CONF_CALLABLE_CONCURRENCY); //default 3
    }

    public void destroy() {
        try {
            long limit = System.currentTimeMillis() + 30 * 1000;// 30 seconds
            executor.shutdown();
            queue.clear();
            while (!executor.awaitTermination(1000, TimeUnit.MILLISECONDS)) {
                log.info("Waiting for executor to shutdown");
                if (System.currentTimeMillis() > limit) {
                    log.warn("Gave up, continuing without waiting for executor to shutdown");
                    break;
                }
            }
        }
        catch (InterruptedException ex) {
            log.warn(ex);
        }
    }

    public Class<? extends Service> getInterface() {
        return CallableQueueService.class;
    }


    public synchronized int queueSize() {
        return queue.size();
    }


    private synchronized boolean queue(CallableWrapper wrapper, boolean ignoreQueueSize) {
        if (!ignoreQueueSize && queue.size() >= queueSize) {
            log.warn("queue full, ignoring queuing for [{0}]", wrapper.getElement().getKey());
            return false;
        }
        if (!executor.isShutdown()) {
            if (wrapper.filterDuplicates()) {
                wrapper.addToUniqueCallables();
                try {
                    executor.execute(wrapper);
                } catch (Throwable ree) {
                    wrapper.removeFromUniqueCallables();
                    throw new RuntimeException(ree);
                }
            }
        }
        else {
            log.warn("Executor shutting down, ignoring queueing of [{0}]", wrapper.getElement().getKey());
        }
        return true;
    }

    public boolean queue(XCallable<?> callable) {
        return queue(callable, 0);
    }

    public boolean queueSerial(List<? extends XCallable<?>> callables) {
        return queueSerial(callables, 0);
    }

    public synchronized boolean queue(XCallable<?> callable, long delay) {
        if (callable == null) {
            return true;
        }
        boolean queued = false;
        if (Services.get().getSystemMode() == SubmitClient.SYSTEM_MODE.SAFEMODE) {
            log.warn("[queue] System is in SAFEMODE. Hence no callable is queued. current queue size " + queue.size());
        }
        else {
            checkInterruptTypes(callable);
            queued = queue(new CallableWrapper(callable, delay), false);
            if (queued) {
                incrCounter(INSTR_QUEUED_COUNTER, 1);
            }
            else {
                log.warn("Could not queue callable");
            }
        }
        return queued;
    }


    public synchronized boolean queueSerial(List<? extends XCallable<?>> callables, long delay) {
        boolean queued;
        if (callables == null || callables.size() == 0) {
            queued = true;
        }
        else if (callables.size() == 1) {
            queued = queue(callables.get(0), delay);
        }
        else {
            XCallable<?> callable = new CompositeCallable(callables);
            queued = queue(callable, delay);
            if (queued) {
                incrCounter(INSTR_QUEUED_COUNTER, callables.size());
            }
        }
        return queued;
    }


    public Set<XCallable<?>> checkInterrupts(String lockKey) {

        if (lockKey != null) {
            return interruptCommandsMap.remove(lockKey);
        }
        return null;
    }

    public void checkInterruptTypes(XCallable<?> callable) {
        if ((callable instanceof CompositeCallable) && (((CompositeCallable) callable).getCallables() != null)) {
            for (XCallable<?> singleCallable : ((CompositeCallable) callable).getCallables()) {
                if (INTERRUPT_TYPES.contains(singleCallable.getType())) {
                    insertCallableIntoInterruptMap(singleCallable);
                }
            }
        }
        else if (INTERRUPT_TYPES.contains(callable.getType())) {
            insertCallableIntoInterruptMap(callable);
        }
    }

    public void insertCallableIntoInterruptMap(XCallable<?> callable) {
        if (interruptCommandsMap.size() < interruptMapMaxSize) {
            Set<XCallable<?>> newSet = Collections.synchronizedSet(new LinkedHashSet<XCallable<?>>());
            Set<XCallable<?>> interruptSet = interruptCommandsMap.putIfAbsent(callable.getEntityKey(), newSet);
            if (interruptSet == null) {
                interruptSet = newSet;
            }
            if (interruptSet.add(callable)) {
                log.trace("Inserting an interrupt element [{0}] to the interrupt map", callable.toString());
            } else {
                log.trace("Interrupt element [{0}] already present", callable.toString());
            }
        }
        else {
            log.warn(
                    "The interrupt map reached max size of [{0}], an interrupt element [{1}] will not added to the map [{1}]",
                    interruptCommandsMap.size(), callable.toString());
        }
    }

    public List<String> getQueueDump() {
        List<String> list = new ArrayList<String>();
        for (PriorityDelayQueue.QueueElement<CallableWrapper> qe : queue) {
            if (qe.toString() == null) {
                continue;
            }
            list.add(qe.toString());
        }
        return list;
    }

    public List<String> getUniqueDump() {
        List<String> list = new ArrayList<String>();
        for (Map.Entry<String, Date> entry : uniqueCallables.entrySet()) {
            list.add(entry.toString());
        }
        return list;
    }



    class CallableWrapper extends PriorityDelayQueue.QueueElement<XCallable<?>> implements Runnable {
        private Instrumentation.Cron cron;

        public CallableWrapper(XCallable<?> callable, long delay) {
            super(callable, callable.getPriority(), delay, TimeUnit.MILLISECONDS);
            cron = new Instrumentation.Cron();
            cron.start();
        }

        public void run() {
            XCallable<?> callable = null;
            try {
                removeFromUniqueCallables();
                if (Services.get().getSystemMode() == SubmitClient.SYSTEM_MODE.SAFEMODE) {
                    log.info("Oozie is in SAFEMODE, requeuing callable [{0}] with [{1}]ms delay", getElement().getType(),
                            SAFE_MODE_DELAY);
                    setDelay(SAFE_MODE_DELAY, TimeUnit.MILLISECONDS);
                    queue(this, true);
                    return;
                }
                callable = getElement();
                if (callableBegin(callable)) {
                    cron.stop();
                    addInQueueCron(cron);
                    XLog log = XLog.getLog(getClass());
                    log.trace("executing callable [{0}]", callable.getName());

                    try {
                        callable.call();
                        incrCounter(INSTR_EXECUTED_COUNTER, 1);
                        log.trace("executed callable [{0}]", callable.getName());
                    }
                    catch (Exception ex) {
                        incrCounter(INSTR_FAILED_COUNTER, 1);
                        log.warn("exception callable [{0}], {1}", callable.getName(), ex.getMessage(), ex);
                    }
                }
                else {
                    log.warn("max concurrency for callable [{0}] exceeded, requeueing with [{1}]ms delay", callable
                            .getType(), CONCURRENCY_DELAY);
                    setDelay(CONCURRENCY_DELAY, TimeUnit.MILLISECONDS);
                    queue(this, true);
                    incrCounter(callable.getType() + "#exceeded.concurrency", 1);
                }
            }
            catch (Throwable t) {
                incrCounter(INSTR_FAILED_COUNTER, 1);
                log.warn("exception callable [{0}], {1}", callable == null ? "N/A" : callable.getName(),
                        t.getMessage(), t);
            }
            finally {
                if (callable != null) {
                    callableEnd(callable);
                }
            }
        }

        /**
         * Filter the duplicate callables from the list before queue this.
         * <p>
         * If it is single callable, checking if key is in unique map or not.
         * <p>
         * If it is composite callable, remove duplicates callables from the composite.
         *
         * @return true if this callable should be queued
         */
        public boolean filterDuplicates() {
            XCallable<?> callable = getElement();
            if (callable instanceof CompositeCallable) {
                return ((CompositeCallable) callable).removeDuplicates();
            }
            else {
                return uniqueCallables.containsKey(callable.getKey()) == false;
            }
        }

        /**
         * Add the keys to the set
         */
        public void addToUniqueCallables() {
            XCallable<?> callable = getElement();
            if (callable instanceof CompositeCallable) {
                ((CompositeCallable) callable).addToUniqueCallables();
            }
            else {
                ((ConcurrentHashMap<String, Date>) uniqueCallables).putIfAbsent(callable.getKey(), new Date());
            }
        }

        /**
         * Remove the keys from the set
         */
        public void removeFromUniqueCallables() {
            XCallable<?> callable = getElement();
            if (callable instanceof CompositeCallable) {
                ((CompositeCallable) callable).removeFromUniqueCallables();
            }
            else {
                uniqueCallables.remove(callable.getKey());
            }
        }
    }


    class CompositeCallable implements XCallable<Void> {
        private List<XCallable<?>> callables;
        private String name;
        private int priority;
        private long createdTime;

        public CompositeCallable(List<? extends XCallable<?>> callables) {
            this.callables = new ArrayList<XCallable<?>>(callables);
            priority = 0;
            createdTime = Long.MAX_VALUE;
            StringBuilder sb = new StringBuilder();
            String separator = "[";
            for (XCallable<?> callable : callables) {
                priority = Math.max(priority, callable.getPriority());
                createdTime = Math.min(createdTime, callable.getCreatedTime());
                sb.append(separator).append(callable.getName());
                separator = ",";
            }
            sb.append("]");
            name = sb.toString();
        }


        public String getName() {
            return name;
        }


        public String getType() {
            return "#composite#" + callables.get(0).getType();
        }


        public String getKey() {
            return "#composite#" + callables.get(0).getKey();
        }


        public String getEntityKey() {
            return "#composite#" + callables.get(0).getEntityKey();
        }


        public int getPriority() {
            return priority;
        }


        public long getCreatedTime() {
            return createdTime;
        }


        public void setInterruptMode(boolean mode) {
        }


        public boolean inInterruptMode() {
            return false;
        }

        public List<XCallable<?>> getCallables() {
            return this.callables;
        }

        public Void call() throws Exception {
            XLog log = XLog.getLog(getClass());

            for (XCallable<?> callable : callables) {
                log.trace("executing callable [{0}]", callable.getName());
                try {
                    callable.call();
                    incrCounter(INSTR_EXECUTED_COUNTER, 1);
                    log.trace("executed callable [{0}]", callable.getName());
                }
                catch (Exception ex) {
                    incrCounter(INSTR_FAILED_COUNTER, 1);
                    log.warn("exception callable [{0}], {1}", callable.getName(), ex.getMessage(), ex);
                }
            }

            // ticking -1 not to count the call to the composite callable
            incrCounter(INSTR_EXECUTED_COUNTER, -1);
            return null;
        }

        /*
         * (non-Javadoc)
         *
         * @see java.lang.Object#toString()
         */
        @Override
        public String toString() {
            if (callables.size() == 0) {
                return null;
            }
            StringBuilder sb = new StringBuilder();
            int size = callables.size();
            for (int i = 0; i < size; i++) {
                XCallable<?> callable = callables.get(i);
                sb.append("(");
                sb.append(callable.toString());
                if (i + 1 == size) {
                    sb.append(")");
                }
                else {
                    sb.append("),");
                }
            }
            return sb.toString();
        }

        /**
         * Remove the duplicate callables from the list before queue them
         *
         * @return true if callables should be queued
         */
        public boolean removeDuplicates() {
            Set<String> set = new HashSet<String>();
            List<XCallable<?>> filteredCallables = new ArrayList<XCallable<?>>();
            if (callables.size() == 0) {
                return false;
            }
            for (XCallable<?> callable : callables) {
                if (!uniqueCallables.containsKey(callable.getKey()) && !set.contains(callable.getKey())) {
                    filteredCallables.add(callable);
                    set.add(callable.getKey());
                }
            }
            callables = filteredCallables;
            if (callables.size() == 0) {
                return false;
            }
            return true;
        }

        /**
         * Add the keys to the set
         */
        public void addToUniqueCallables() {
            for (XCallable<?> callable : callables) {
                ((ConcurrentHashMap<String, Date>) uniqueCallables).putIfAbsent(callable.getKey(), new Date());
            }
        }

        /**
         * Remove the keys from the set
         */
        public void removeFromUniqueCallables() {
            for (XCallable<?> callable : callables) {
                uniqueCallables.remove(callable.getKey());
            }
        }
    }


}
