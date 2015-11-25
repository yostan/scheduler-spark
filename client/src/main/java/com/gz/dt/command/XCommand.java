package com.gz.dt.command;

import com.gz.dt.exception.CommandException;
import com.gz.dt.exception.PreconditionException;
import com.gz.dt.exception.XException;
import com.gz.dt.service.*;
import com.gz.dt.util.ErrorCode;
import com.gz.dt.util.FaultInjection;
import com.gz.dt.util.Instrumentation;
import com.gz.dt.lock.LockToken;
import com.gz.dt.util.XLog;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by naonao on 2015/10/27.
 */
public abstract class XCommand<T> implements XCallable<T> {

    public static final String DEFAULT_LOCK_TIMEOUT = "oozie.command.default.lock.timeout";
    public static final String INSTRUMENTATION_GROUP = "commands";
    public static final String DEFAULT_REQUEUE_DELAY = "oozie.command.default.requeue.delay";

    public XLog LOG = XLog.getLog(getClass());

    private String key;
    private String name;
    private int priority;
    private String type;
    private long createdTime;
    private LockToken lock;
    private AtomicBoolean used = new AtomicBoolean(false);
    private boolean inInterrupt = false;
    private boolean isSynchronous = false;

    private Map<Long, List<XCommand<?>>> commandQueue;
    protected boolean dryrun = false;
    protected Instrumentation instrumentation;

//    protected static EventHandlerService eventService;


    public XCommand(String name, String type, int priority) {
        this.name = name;
        this.type = type;
        this.priority = priority;
        this.key = name + "_" + UUID.randomUUID();
        createdTime = System.currentTimeMillis();
        instrumentation = Services.get().get(InstrumentationService.class).get();
//        eventService = Services.get().get(EventHandlerService.class);
    }

    public XCommand(String name, String type, int priority, boolean dryrun) {
        this(name, type, priority);
        this.dryrun = dryrun;
    }

    protected void setLogInfo() {
    }

    public String getName() {
        return name;
    }

    public String getType() {
        return type;
    }

    public int getPriority() {
        return priority;
    }

    public long getCreatedTime() {
        return createdTime;
    }

    protected void queue(XCommand<?> command) {
        queue(command, 0);
    }

    protected void queue(XCommand<?> command, long msDelay) {
        if (commandQueue == null) {
            commandQueue = new HashMap<Long, List<XCommand<?>>>();
        }
        List<XCommand<?>> list = commandQueue.get(msDelay);
        if (list == null) {
            list = new ArrayList<XCommand<?>>();
            commandQueue.put(msDelay, list);
        }
        list.add(command);
    }


    private void acquireLock() throws InterruptedException, CommandException {
        if (getEntityKey() == null) {
            // no lock for null entity key
            return;
        }
        lock = Services.get().get(MemoryLocksService.class).getWriteLock(getEntityKey(), getLockTimeOut());
        if (lock == null) {
            instrumentation.incr(INSTRUMENTATION_GROUP, getName() + ".lockTimeOut", 1);
            if (isReQueueRequired()) {
                //if not acquire the lock, re-queue itself with default delay
                queue(this, getRequeueDelay());
                LOG.debug("Could not get lock [{0}], timed out [{1}]ms, and requeue itself [{2}]", this.toString(), getLockTimeOut(), getName());
            } else {
                throw new CommandException(ErrorCode.E0606, this.toString(), getLockTimeOut());
            }
        } else {
            LOG.debug("Acquired lock for [{0}] in [{1}]", getEntityKey(), getName());
        }
    }


    private void releaseLock() {
        if (lock != null) {
            lock.release();
            LOG.debug("Released lock for [{0}] in [{1}]", getEntityKey(), getName());
        }
    }


    protected long getLockTimeOut() {
        return Services.get().getConf().getLong(DEFAULT_LOCK_TIMEOUT, 5 * 1000);
    }

    protected abstract boolean isLockRequired();

    public abstract String getEntityKey();

    protected boolean isReQueueRequired() {
        return true;
    }

    protected void eagerLoadState() throws CommandException{
    }

    protected void eagerVerifyPrecondition() throws CommandException,PreconditionException {
    }

    protected abstract void loadState() throws CommandException;

    protected abstract void verifyPrecondition() throws CommandException,PreconditionException;

    protected abstract T execute() throws CommandException;

    protected Instrumentation getInstrumentation() {
        return instrumentation;
    }

    public void resetUsed() {
        this.used.set(false);
    }

    protected long getRequeueDelay() {
        return ConfigurationService.getLong(DEFAULT_REQUEUE_DELAY);
    }

    public String getKey(){
        return this.key;
    }

    public void setInterruptMode(boolean mode) {
        this.inInterrupt = mode;
    }

    public boolean inInterruptMode() {
        return this.inInterrupt;
    }

    public XLog getLog() {
        return LOG;
    }

    public String toString() {
        return getKey();
    }


    protected void executeInterrupts() {
        CallableQueueService callableQueueService = Services.get().get(CallableQueueService.class);
        // getting all the list of interrupts to be executed
        Set<XCallable<?>> callables = callableQueueService.checkInterrupts(this.getEntityKey());

        if (callables != null) {
            // executing the list of interrupts in the given order of insertion
            // in the list
            for (XCallable<?> callable : callables) {
                LOG.trace("executing interrupt callable [{0}]", callable.getName());
                try {
                    // executing the callable in interrupt mode
                    callable.setInterruptMode(true);
                    callable.call();
                    LOG.trace("executed interrupt callable [{0}]", callable.getName());
                }
                catch (Exception ex) {
                    LOG.warn("exception interrupt callable [{0}], {1}", callable.getName(), ex.getMessage(), ex);
                }
                finally {
                    // reseting the interrupt mode to false after the command is
                    // executed
                    callable.setInterruptMode(false);
                }
            }
        }
    }


    public final T call() throws CommandException {
        setLogInfo();
        if (CallableQueueService.INTERRUPT_TYPES.contains(this.getType()) && used.get()) {
            LOG.debug("Command [{0}] key [{1}]  already used for [{2}]", getName(), getEntityKey(), this.toString());
            return null;
        }

        commandQueue = null;
        instrumentation.incr(INSTRUMENTATION_GROUP, getName() + ".executions", 1);
        Instrumentation.Cron callCron = new Instrumentation.Cron();
        try {
            callCron.start();
            if (!isSynchronous) {
                eagerLoadState();
                eagerVerifyPrecondition();
            }
            try {
                T ret = null;
                if (!isSynchronous && isLockRequired() && !this.inInterruptMode()) {
                    Instrumentation.Cron acquireLockCron = new Instrumentation.Cron();
                    acquireLockCron.start();
                    acquireLock();
                    acquireLockCron.stop();
                    instrumentation.addCron(INSTRUMENTATION_GROUP, getName() + ".acquireLock", acquireLockCron);
                }
                // executing interrupts only in case of the lock required commands
                if (lock != null) {
                    this.executeInterrupts();
                }

                if (isSynchronous || !isLockRequired() || (lock != null) || this.inInterruptMode()) {
                    if (CallableQueueService.INTERRUPT_TYPES.contains(this.getType())
                            && !used.compareAndSet(false, true)) {
                        LOG.debug("Command [{0}] key [{1}]  already executed for [{2}]", getName(), getEntityKey(), this.toString());
                        return null;
                    }
                    LOG.trace("Load state for [{0}]", getEntityKey());
                    loadState();
                    LOG.trace("Precondition check for command [{0}] key [{1}]", getName(), getEntityKey());
                    verifyPrecondition();
                    LOG.debug("Execute command [{0}] key [{1}]", getName(), getEntityKey());
                    Instrumentation.Cron executeCron = new Instrumentation.Cron();
                    executeCron.start();
                    ret = execute();
                    executeCron.stop();
                    instrumentation.addCron(INSTRUMENTATION_GROUP, getName() + ".execute", executeCron);
                }
                if (commandQueue != null) {
                    CallableQueueService callableQueueService = Services.get().get(CallableQueueService.class);
                    for (Map.Entry<Long, List<XCommand<?>>> entry : commandQueue.entrySet()) {
                        LOG.debug("Queuing [{0}] commands with delay [{1}]ms", entry.getValue().size(), entry.getKey());
                        if (!callableQueueService.queueSerial(entry.getValue(), entry.getKey())) {
                            LOG.warn("Could not queue [{0}] commands with delay [{1}]ms, queue full", entry.getValue()
                                    .size(), entry.getKey());
                        }
                    }
                }
                return ret;
            }
            finally {
                if (!isSynchronous && isLockRequired() && !this.inInterruptMode()) {
                    releaseLock();
                }
            }
        }
        catch(PreconditionException pex){
            LOG.warn(pex.getMessage().toString() + ", Error Code: " + pex.getErrorCode().toString());
            instrumentation.incr(INSTRUMENTATION_GROUP, getName() + ".preconditionfailed", 1);
            return null;
        }
        catch (XException ex) {
            LOG.error("XException, ", ex);
            instrumentation.incr(INSTRUMENTATION_GROUP, getName() + ".xexceptions", 1);
            if (ex instanceof CommandException) {
                throw (CommandException) ex;
            }
            else {
                throw new CommandException(ex);
            }
        }
        catch (Exception ex) {
            LOG.error("Exception, ", ex);
            instrumentation.incr(INSTRUMENTATION_GROUP, getName() + ".exceptions", 1);
            throw new CommandException(ErrorCode.E0607, getName(), ex.getMessage(), ex);
        }
        catch (Error er) {
            LOG.error("Error, ", er);
            instrumentation.incr(INSTRUMENTATION_GROUP, getName() + ".errors", 1);
            throw er;
        }
        finally {
            FaultInjection.deactivate("org.apache.oozie.command.SkipCommitFaultInjection");
            callCron.stop();
            instrumentation.addCron(INSTRUMENTATION_GROUP, getName() + ".call", callCron);
        }
    }


    public final T call(String callerEntityKey) throws CommandException {
        if (!callerEntityKey.equals(this.getEntityKey())) {
            throw new CommandException(ErrorCode.E0607, "Entity Keys mismatch during synchronous call", "caller="
                    + callerEntityKey + ", callee=" + getEntityKey());
        }
        isSynchronous = true; //setting to true so lock acquiring and release is not repeated
        LOG.trace("Executing synchronously command [{0}] on job [{1}]", this.getName(), this.getKey());
        return call();
    }



}
