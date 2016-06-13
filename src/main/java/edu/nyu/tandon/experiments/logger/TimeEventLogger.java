package edu.nyu.tandon.experiments.logger;

import java.io.*;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class TimeEventLogger extends FileEventLogger {

    private long timestamp = 0;

    public TimeEventLogger(OutputStream o) {
        super(o);
    }

    public TimeEventLogger(File f) throws IOException {
        super(f);
    }

    public TimeEventLogger(String f) throws IOException {
        super(f);
    }

    @Override
    public void onStart(Object... o) {
        timestamp = System.currentTimeMillis();
    }

    @Override
    public void onEnd(Object... o) {
        long elapsed = System.currentTimeMillis() - timestamp;
        log(elapsed);
    }
}
