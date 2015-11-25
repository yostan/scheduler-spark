package com.gz.dt.command;

import java.util.concurrent.Callable;

/**
 * Created by naonao on 2015/10/27.
 */
public interface XCallable<T> extends Callable<T> {

    public String getName();

    public int getPriority();

    public String getType();

    public long getCreatedTime();

    public String getKey();

    public String getEntityKey();

    public void setInterruptMode(boolean mode);

    public boolean inInterruptMode();
}
