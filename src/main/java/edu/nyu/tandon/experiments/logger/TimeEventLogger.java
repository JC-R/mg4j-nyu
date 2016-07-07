package edu.nyu.tandon.experiments.logger;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class TimeEventLogger extends FileEventLogger {

    private long queriesProcessed = 0;
    private long timestamp = 0;

    @Override
    public String header() {
        return "id,time";
    }

    public TimeEventLogger(OutputStream o) throws IOException {
        super(o);
    }

    public TimeEventLogger(File f) throws IOException {
        super(f);
    }

    public TimeEventLogger(String f) throws IOException {
        super(f);
    }

    @Override
    public void onStart(Iterable<String> query) {
        timestamp = System.currentTimeMillis();
    }

    @Override
    public void onEnd(Iterable<Long> results) {
        long elapsed = System.currentTimeMillis() - timestamp;
        log(String.format("%d,%d", queriesProcessed++, elapsed));
    }
}
