package edu.nyu.tandon.experiments.ml.cluster.logger;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class TimeClusterEventLogger extends FileClusterEventLogger {

    private long timestamp = 0;

    public TimeClusterEventLogger(String f) throws IOException {
        super(f);
    }

    @Override
    public String column() {
        return "time";
    }

    @Override
    public void onStart(long id, int cluster, Iterable<String> query) {
        timestamp = System.currentTimeMillis();
    }

    @Override
    public void onEnd(long id, int cluster, Iterable<Long> results) {
        long elapsed = System.currentTimeMillis() - timestamp;
        log(id, cluster, String.valueOf(elapsed));
    }
}
