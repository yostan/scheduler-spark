package com.gz.dt.spark;

/**
 * Created by naonao on 2015/10/21.
 */
public abstract class LauncherMain {

    protected static void run(Class<? extends LauncherMain> klass, String[] args) throws Exception {
        LauncherMain main = klass.newInstance();
        main.run(args);
    }

    protected abstract void run(String[] args) throws Exception;
}
