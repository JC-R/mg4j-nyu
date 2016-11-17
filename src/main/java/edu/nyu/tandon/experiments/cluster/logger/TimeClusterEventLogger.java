package edu.nyu.tandon.experiments.cluster.logger;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class TimeClusterEventLogger extends FileClusterEventLogger {

    private long timestamp = 0;

    @Override
    public String column() {
        return "time";
    }

    public TimeClusterEventLogger(String f) throws IOException {
        super(f);
    }

    @Override
    public void onStart(long id, Iterable<String> query) {
        timestamp = System.currentTimeMillis();
    }

    @Override
    public void onEnd(long id, Iterable<Object> results) {
        long elapsed = System.currentTimeMillis() - timestamp;
        log(id, String.valueOf(elapsed));
    }
}