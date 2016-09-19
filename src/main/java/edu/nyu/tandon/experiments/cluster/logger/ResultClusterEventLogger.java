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

    @Override
    public String column() {
        return "results";
    }

    public ResultClusterEventLogger(String f) throws IOException {
        super(f);
    }

    @Override
    public void onStart(long id, Iterable<String> query) {
    }

    @Override
    public void onEnd(long id, Iterable<Object> results) {
        log(id, Joiner.on(" ").join(results));
    }
}
