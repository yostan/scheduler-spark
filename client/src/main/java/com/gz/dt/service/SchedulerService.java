package com.gz.dt.service;

import com.gz.dt.client.SubmitClient;
import com.gz.dt.exception.ServiceException;
import com.gz.dt.util.XLog;
import org.apache.hadoop.conf.Configuration;

import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created by naonao on 2015/10/27.
 */
public class SchedulerService implements Service {
    public static final String CONF_PREFIX = Service.CONF_PREFIX + "SchedulerService.";

    public static final String SCHEDULER_THREADS = CONF_PREFIX + "threads";

    private final XLog log = XLog.getLog(getClass());

    private ScheduledExecutorService scheduler;


    public void init(Services services) throws ServiceException {
        scheduler = new ScheduledThreadPoolExecutor(getSchedulableThreads(services.getConf()));
    }


    public ScheduledExecutorService getScheduler() {
        return scheduler;
    }


    public int getSchedulableThreads(Configuration conf) {
        return ConfigurationService.getInt(conf, SCHEDULER_THREADS);  //default 10
    }


    public enum Unit {
        MILLISEC(1),
        SEC(1000),
        MIN(1000 * 60),
        HOUR(1000 * 60 * 60);

        private long millis;

        private Unit(long millis) {
            this.millis = millis;
        }

        private long getMillis() {
            return millis;
        }

    }


    public void schedule(final Callable<Void> callable, long delay, long interval, Unit unit) {
        log.trace("Scheduling callable [{0}], interval [{1}] seconds, delay [{2}] in [{3}]",
                callable.getClass(), delay, interval, unit);
        Runnable r = new Runnable() {
            public void run() {
                if (Services.get().getSystemMode() == SubmitClient.SYSTEM_MODE.SAFEMODE) {
                    log.trace("schedule[run/callable] System is in SAFEMODE. Therefore nothing will run");
                    return;
                }
                try {
                    callable.call();
                }
                catch (Exception ex) {
                    log.warn("Error executing callable [{0}], {1}", callable.getClass().getSimpleName(),
                            ex.getMessage(), ex);
                }
            }
        };
        if (!scheduler.isShutdown()) {
            scheduler.scheduleWithFixedDelay(r, delay * unit.getMillis(), interval * unit.getMillis(),
                    TimeUnit.MILLISECONDS);
        }
        else {
            log.warn("Scheduler shutting down, ignoring scheduling of [{0}]", callable.getClass());
        }
    }


    public void schedule(final Runnable runnable, long delay, long interval, Unit unit) {
        log.trace("Scheduling runnable [{0}], interval [{1}], delay [{2}] in [{3}]",
                runnable.getClass(), delay, interval, unit);
        Runnable r = new Runnable() {
            public void run() {
                if (Services.get().getSystemMode() == SubmitClient.SYSTEM_MODE.SAFEMODE) {
                    log.trace("schedule[run/Runnable] System is in SAFEMODE. Therefore nothing will run");
                    return;
                }
                try {
                    runnable.run();
                }
                catch (Exception ex) {
                    log.warn("Error executing runnable [{0}], {1}", runnable.getClass().getSimpleName(),
                            ex.getMessage(), ex);
                }
            }
        };
        if (!scheduler.isShutdown()) {
            scheduler.scheduleWithFixedDelay(r, delay * unit.getMillis(), interval * unit.getMillis(),
                    TimeUnit.MILLISECONDS);
        }
        else {
            log.warn("Scheduler shutting down, ignoring scheduling of [{0}]", runnable.getClass());
        }
    }


    public void schedule(final Runnable runnable, long delay, Unit unit) {
        log.trace("Scheduling runnable [{0}], delay [{1}] in [{2}]",
                runnable.getClass(), delay, unit);
        Runnable r = new Runnable() {
            public void run() {
                if (Services.get().getSystemMode() == SubmitClient.SYSTEM_MODE.SAFEMODE) {
                    log.trace("schedule[run/Runnable] System is in SAFEMODE. Therefore nothing will run");
                    return;
                }
                try {
                    runnable.run();
                }
                catch (Exception ex) {
                    log.warn("Error executing runnable [{0}], {1}", runnable.getClass().getSimpleName(),
                            ex.getMessage(), ex);
                }
            }
        };
        if (!scheduler.isShutdown()) {
            scheduler.schedule(r, delay * unit.getMillis(), TimeUnit.MILLISECONDS);
        }
        else {
            log.warn("Scheduler shutting down, ignoring scheduling of [{0}]", runnable.getClass());
        }
    }



    public void destroy() {
        try {
            long limit = System.currentTimeMillis() + 30 * 1000;// 30 seconds
            scheduler.shutdownNow();
            while (!scheduler.awaitTermination(1000, TimeUnit.MILLISECONDS)) {
                log.info("Waiting for scheduler to shutdown");
                if (System.currentTimeMillis() > limit) {
                    log.warn("Gave up, continuing without waiting for scheduler to shutdown");
                    break;
                }
            }
        }
        catch (InterruptedException ex) {
            log.warn(ex);
        }

    }

    public Class<? extends Service> getInterface() {
        return SchedulerService.class;
    }
}
