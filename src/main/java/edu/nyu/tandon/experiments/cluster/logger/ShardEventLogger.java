package edu.nyu.tandon.experiments.cluster.logger;

import java.io.IOException;
import java.util.Map;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public abstract class ShardEventLogger extends FileClusterEventLogger {

    private String selector;

    public ShardEventLogger(String f) throws IOException {
        super(f);
    }

    @Override
    public void onStart(long id, Iterable<String> query) {

    }

    @Override
    public void onEnd(long id, Iterable<Object> results) {

    }

    public abstract void onEnd(long id, Map<Integer, Double> shardScores);
}
