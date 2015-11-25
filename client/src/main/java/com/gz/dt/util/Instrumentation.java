package com.gz.dt.util;

import com.gz.dt.service.ConfigurationService;
import com.gz.dt.service.Services;
import org.apache.hadoop.conf.Configuration;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by naonao on 2015/10/26.
 */
public class Instrumentation {

    private ScheduledExecutorService scheduler;
    private Lock counterLock;
    private Lock timerLock;
    private Lock variableLock;
    private Lock samplerLock;
    private Map<String, Map<String, Map<String, Object>>> all;
    private Map<String, Map<String, Element<Long>>> counters;
    private Map<String, Map<String, Element<Timer>>> timers;
    private Map<String, Map<String, Element<Variable>>> variables;
    private Map<String, Map<String, Element<Double>>> samplers;


    public Instrumentation() {
        counterLock = new ReentrantLock();
        timerLock = new ReentrantLock();
        variableLock = new ReentrantLock();
        samplerLock = new ReentrantLock();
        all = new LinkedHashMap<String, Map<String, Map<String, Object>>>();
        counters = new ConcurrentHashMap<String, Map<String, Element<Long>>>();
        timers = new ConcurrentHashMap<String, Map<String, Element<Timer>>>();
        variables = new ConcurrentHashMap<String, Map<String, Element<Variable>>>();
        samplers = new ConcurrentHashMap<String, Map<String, Element<Double>>>();
        all.put("variables", (Map<String, Map<String, Object>>) (Object) variables);
        all.put("samplers", (Map<String, Map<String, Object>>) (Object) samplers);
        all.put("counters", (Map<String, Map<String, Object>>) (Object) counters);
        all.put("timers", (Map<String, Map<String, Object>>) (Object) timers);
    }


    public void setScheduler(ScheduledExecutorService scheduler) {
        this.scheduler = scheduler;
    }

    public void addCron(String group, String name, Cron cron) {
        Map<String, Element<Timer>> map = timers.get(group);
        if (map == null) {
            try {
                timerLock.lock();
                map = timers.get(group);
                if (map == null) {
                    map = new HashMap<String, Element<Timer>>();
                    timers.put(group, map);
                }
            }
            finally {
                timerLock.unlock();
            }
        }
        Timer timer = (Timer) map.get(name);
        if (timer == null) {
            try {
                timerLock.lock();
                timer = (Timer) map.get(name);
                if (timer == null) {
                    timer = new Timer();
                    map.put(name, timer);
                }
            }
            finally {
                timerLock.unlock();
            }
        }
        timer.addCron(cron);
    }


    public void incr(String group, String name, long count) {
        Map<String, Element<Long>> map = counters.get(group);
        if (map == null) {
            try {
                counterLock.lock();
                map = counters.get(group);
                if (map == null) {
                    map = new HashMap<String, Element<Long>>();
                    counters.put(group, map);
                }
            }
            finally {
                counterLock.unlock();
            }
        }
        Counter counter = (Counter) map.get(name);
        if (counter == null) {
            try {
                counterLock.lock();
                counter = (Counter) map.get(name);
                if (counter == null) {
                    counter = new Counter();
                    map.put(name, counter);
                }
            }
            finally {
                counterLock.unlock();
            }
        }
        counter.addAndGet(count);
    }


    public void addVariable(String group, String name, Variable variable) {
        Map<String, Element<Variable>> map = variables.get(group);
        if (map == null) {
            try {
                variableLock.lock();
                map = variables.get(group);
                if (map == null) {
                    map = new HashMap<String, Element<Variable>>();
                    variables.put(group, map);
                }
            }
            finally {
                variableLock.unlock();
            }
        }
        if (map.containsKey(name)) {
            throw new RuntimeException(XLog.format("Variable group=[{0}] name=[{1}] already defined", group, name));
        }
        map.put(name, variable);
    }

    public Map<String, String> getJavaSystemProperties() {
        return (Map<String, String>) (Object) System.getProperties();
    }


    public Map<String, String> getOSEnv() {
        return System.getenv();
    }


    public Map<String, String> getConfiguration() {
        final Configuration maskedConf = Services.get().get(ConfigurationService.class).getMaskedConfiguration();

        return new Map<String, String>() {
            public int size() {
                return maskedConf.size();
            }

            public boolean isEmpty() {
                return maskedConf.size() == 0;
            }

            public boolean containsKey(Object o) {
                return maskedConf.get((String) o) != null;
            }

            public boolean containsValue(Object o) {
                throw new UnsupportedOperationException();
            }

            public String get(Object o) {
                return maskedConf.get((String) o);
            }

            public String put(String s, String s1) {
                throw new UnsupportedOperationException();
            }

            public String remove(Object o) {
                throw new UnsupportedOperationException();
            }

            public void putAll(Map<? extends String, ? extends String> map) {
                throw new UnsupportedOperationException();
            }

            public void clear() {
                throw new UnsupportedOperationException();
            }

            public Set<String> keySet() {
                Set<String> set = new LinkedHashSet<String>();
                for (Entry<String, String> entry : maskedConf) {
                    set.add(entry.getKey());
                }
                return set;
            }

            public Collection<String> values() {
                Set<String> set = new LinkedHashSet<String>();
                for (Entry<String, String> entry : maskedConf) {
                    set.add(entry.getValue());
                }
                return set;
            }

            public Set<Entry<String, String>> entrySet() {
                Set<Entry<String, String>> set = new LinkedHashSet<Entry<String, String>>();
                for (Entry<String, String> entry : maskedConf) {
                    set.add(entry);
                }
                return set;
            }
        };
    }


    public Map<String, Map<String, Element<Long>>> getCounters() {
        return counters;
    }

    public Map<String, Map<String, Element<Timer>>> getTimers() {
        return timers;
    }

    public Map<String, Map<String, Element<Variable>>> getVariables() {
        return variables;
    }

    public Map<String, Map<String, Map<String, Object>>> getAll() {
        return all;
    }


    public String toString() {
        String E = System.getProperty("line.separator");
        StringBuilder sb = new StringBuilder(4096);
        for (String element : all.keySet()) {
            sb.append(element).append(':').append(E);
            List<String> groups = new ArrayList<String>(all.get(element).keySet());
            Collections.sort(groups);
            for (String group : groups) {
                sb.append("  ").append(group).append(':').append(E);
                List<String> names = new ArrayList<String>(all.get(element).get(group).keySet());
                Collections.sort(names);
                for (String name : names) {
                    sb.append("    ").append(name).append(": ").append(((Element) all.get(element).
                            get(group).get(name)).getValue()).append(E);
                }
            }
        }
        return sb.toString();
    }


    public void addSampler(String group, String name, int period, int interval, Variable<Long> variable) {
        if (scheduler == null) {
            throw new IllegalStateException("scheduler not set, cannot sample");
        }
        try {
            samplerLock.lock();
            Map<String, Element<Double>> map = samplers.get(group);
            if (map == null) {
                map = samplers.get(group);
                if (map == null) {
                    map = new HashMap<String, Element<Double>>();
                    samplers.put(group, map);
                }
            }
            if (map.containsKey(name)) {
                throw new RuntimeException(XLog.format("Sampler group=[{0}] name=[{1}] already defined", group, name));
            }
            Sampler sampler = new Sampler(period, interval, variable);
            map.put(name, sampler);
            scheduler.scheduleAtFixedRate(sampler, 0, sampler.getSamplingInterval(), TimeUnit.SECONDS);
        }
        finally {
            samplerLock.unlock();
        }
    }


    public Map<String, Map<String, Element<Double>>> getSamplers() {
        return samplers;
    }


    public interface Element<T> {

        /**
         * Return the snapshot value of the Intrumentation element.
         *
         * @return the snapshot value of the Intrumentation element.
         */
        T getValue();
    }


    public static class Timer implements Element<Timer> {
        Lock lock = new ReentrantLock();
        private long ownTime;
        private long totalTime;
        private long ticks;
        private long ownSquareTime;
        private long totalSquareTime;
        private long ownMinTime;
        private long ownMaxTime;
        private long totalMinTime;
        private long totalMaxTime;

        /**
         * Timer constructor. <p> It is project private for test purposes.
         */
        Timer() {
        }

        /**
         * Return the String representation of the timer value.
         *
         * @return the String representation of the timer value.
         */
        public String toString() {
            return XLog.format("ticks[{0}] totalAvg[{1}] ownAvg[{2}]", ticks, getTotalAvg(), getOwnAvg());
        }

        /**
         * Return the timer snapshot.
         *
         * @return the timer snapshot.
         */
        public Timer getValue() {
            try {
                lock.lock();
                Timer timer = new Timer();
                timer.ownTime = ownTime;
                timer.totalTime = totalTime;
                timer.ticks = ticks;
                timer.ownSquareTime = ownSquareTime;
                timer.totalSquareTime = totalSquareTime;
                timer.ownMinTime = ownMinTime;
                timer.ownMaxTime = ownMaxTime;
                timer.totalMinTime = totalMinTime;
                timer.totalMaxTime = totalMaxTime;
                return timer;
            }
            finally {
                lock.unlock();
            }
        }

        /**
         * Add a cron to a timer. <p> It is project private for test purposes.
         *
         * @param cron Cron to add.
         */
        void addCron(Cron cron) {
            try {
                lock.lock();
                long own = cron.getOwn();
                long total = cron.getTotal();
                ownTime += own;
                totalTime += total;
                ticks++;
                ownSquareTime += own * own;
                totalSquareTime += total * total;
                if (ticks == 1) {
                    ownMinTime = own;
                    ownMaxTime = own;
                    totalMinTime = total;
                    totalMaxTime = total;
                }
                else {
                    ownMinTime = Math.min(ownMinTime, own);
                    ownMaxTime = Math.max(ownMaxTime, own);
                    totalMinTime = Math.min(totalMinTime, total);
                    totalMaxTime = Math.max(totalMaxTime, total);
                }
            }
            finally {
                lock.unlock();
            }
        }

        /**
         * Return the own accumulated computing time by the timer.
         *
         * @return own accumulated computing time by the timer.
         */
        public long getOwn() {
            return ownTime;
        }

        /**
         * Return the total accumulated computing time by the timer.
         *
         * @return total accumulated computing time by the timer.
         */
        public long getTotal() {
            return totalTime;
        }

        /**
         * Return the number of times a cron was added to the timer.
         *
         * @return the number of times a cron was added to the timer.
         */
        public long getTicks() {
            return ticks;
        }

        /**
         * Return the sum of the square own times. <p> It can be used to calculate the standard deviation.
         *
         * @return the sum of the square own timer.
         */
        public long getOwnSquareSum() {
            return ownSquareTime;
        }

        /**
         * Return the sum of the square total times. <p> It can be used to calculate the standard deviation.
         *
         * @return the sum of the square own timer.
         */
        public long getTotalSquareSum() {
            return totalSquareTime;
        }

        /**
         * Returns the own minimum time.
         *
         * @return the own minimum time.
         */
        public long getOwnMin() {
            return ownMinTime;
        }

        /**
         * Returns the own maximum time.
         *
         * @return the own maximum time.
         */
        public long getOwnMax() {
            return ownMaxTime;
        }

        /**
         * Returns the total minimum time.
         *
         * @return the total minimum time.
         */
        public long getTotalMin() {
            return totalMinTime;
        }

        /**
         * Returns the total maximum time.
         *
         * @return the total maximum time.
         */
        public long getTotalMax() {
            return totalMaxTime;
        }

        /**
         * Returns the own average time.
         *
         * @return the own average time.
         */
        public long getOwnAvg() {
            return (ticks != 0) ? ownTime / ticks : 0;
        }

        /**
         * Returns the total average time.
         *
         * @return the total average time.
         */
        public long getTotalAvg() {
            return (ticks != 0) ? totalTime / ticks : 0;
        }

        /**
         * Returns the total time standard deviation.
         *
         * @return the total time standard deviation.
         */
        public double getTotalStdDev() {
            return evalStdDev(ticks, totalTime, totalSquareTime);
        }

        /**
         * Returns the own time standard deviation.
         *
         * @return the own time standard deviation.
         */
        public double getOwnStdDev() {
            return evalStdDev(ticks, ownTime, ownSquareTime);
        }

        private double evalStdDev(long n, long sn, long ssn) {
            return (n < 2) ? -1 : Math.sqrt((n * ssn - sn * sn) / (n * (n - 1)));
        }

    }


    private static class Counter extends AtomicLong implements Element<Long> {

        /**
         * Return the counter snapshot.
         *
         * @return the counter snapshot.
         */
        public Long getValue() {
            return get();
        }

        /**
         * Return the String representation of the counter value.
         *
         * @return the String representation of the counter value.
         */
        public String toString() {
            return Long.toString(get());
        }

    }


    public interface Variable<T> extends Element<T> {
    }


    private static class Sampler implements Element<Double>, Runnable {
        private Lock lock = new ReentrantLock();
        private int samplingInterval;
        private Variable<Long> variable;
        private long[] values;
        private int current;
        private long valuesSum;
        private double rate;

        public Sampler(int samplingPeriod, int samplingInterval, Variable<Long> variable) {
            this.samplingInterval = samplingInterval;
            this.variable = variable;
            values = new long[samplingPeriod / samplingInterval];
            valuesSum = 0;
            current = -1;
        }

        public int getSamplingInterval() {
            return samplingInterval;
        }

        public void run() {
            try {
                lock.lock();
                long newValue = variable.getValue();
                if (current == -1) {
                    valuesSum = newValue;
                    current = 0;
                    values[current] = newValue;
                }
                else {
                    current = (current + 1) % values.length;
                    valuesSum = valuesSum - values[current] + newValue;
                    values[current] = newValue;
                }
                rate = ((double) valuesSum) / values.length;
            }
            finally {
                lock.unlock();
            }
        }

        public Double getValue() {
            return rate;
        }
    }


    public static class Cron {
        private long start;
        private long end;
        private long lapStart;
        private long own;
        private long total;
        private boolean running;

        /**
         * Creates new Cron, stopped, in zero.
         */
        public Cron() {
            running = false;
        }

        /**
         * Start the cron. It cannot be already started.
         */
        public void start() {
            if (!running) {
                if (lapStart == 0) {
                    lapStart = System.currentTimeMillis();
                    if (start == 0) {
                        start = lapStart;
                        end = start;
                    }
                }
                running = true;
            }
        }

        /**
         * Stops the cron. It cannot be already stopped.
         */
        public void stop() {
            if (running) {
                end = System.currentTimeMillis();
                if (start == 0) {
                    start = end;
                }
                total = end - start;
                if (lapStart > 0) {
                    own += end - lapStart;
                    lapStart = 0;
                }
                running = false;
            }
        }

        /**
         * Return the start time of the cron. It must be stopped.
         *
         * @return the start time of the cron.
         */
        public long getStart() {
            if (running) {
                throw new IllegalStateException("Timer running");
            }
            return start;
        }

        /**
         * Return the end time of the cron.  It must be stopped.
         *
         * @return the end time of the cron.
         */
        public long getEnd() {
            if (running) {
                throw new IllegalStateException("Timer running");
            }
            return end;
        }

        /**
         * Return the total time of the cron. It must be stopped.
         *
         * @return the total time of the cron.
         */
        public long getTotal() {
            if (running) {
                throw new IllegalStateException("Timer running");
            }
            return total;
        }

        /**
         * Return the own time of the cron. It must be stopped.
         *
         * @return the own time of the cron.
         */
        public long getOwn() {
            if (running) {
                throw new IllegalStateException("Timer running");
            }
            return own;
        }

    }
}
