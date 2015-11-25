package com.gz.dt.command;

/**
 * Created by naonao on 2015/10/28.
 */
public class StartXCommand extends SignalXCommand {

    public StartXCommand(String id) {
        super("start", 1, id);
//        InstrumentUtils.incrJobCounter(getName(), 1, getInstrumentation());
    }
}
