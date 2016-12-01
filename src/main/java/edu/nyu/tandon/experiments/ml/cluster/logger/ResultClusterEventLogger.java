package edu.nyu.tandon.experiments.ml.cluster.logger;

import com.google.common.base.Joiner;

import java.io.IOException;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class ResultClusterEventLogger extends FileClusterEventLogger {

    private long queriesProcessed = 0;
    private String clusterHeader = ",cluster";

    public ResultClusterEventLogger(String f) throws IOException {
        super(f);
    }

    @Override
    public String column() {
        return "results";
    }

    @Override
    public void onStart(long id, int cluster, Iterable<String> query) {
    }

    @Override
    public void onEnd(long id, int cluster, Iterable<Long> results) {
        log(id, cluster, Joiner.on(" ").join(results));
    }
}
