package edu.nyu.tandon.experiments.cluster.logger;

import com.google.common.base.Joiner;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class ResultClusterEventLogger extends FileClusterEventLogger {

    private long queriesProcessed = 0;
    private String clusterHeader = ",cluster";

    @Override
    public String column() {
        return "results";
    }

    public ResultClusterEventLogger(String f) throws IOException {
        super(f);
    }

    @Override
    public void onStart(long id, int cluster, Iterable<String> query) {
    }

    @Override
    public void onEnd(long id, int cluster, Iterable<Object> results) {
        log(id, cluster, Joiner.on(" ").join(results));
    }
}
