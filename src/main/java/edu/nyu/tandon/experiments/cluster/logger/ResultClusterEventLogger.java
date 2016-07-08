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

//    public ResultClusterEventLogger(OutputStream o) throws IOException {
//        super(o);
//    }
//
//    public ResultClusterEventLogger(File f) throws IOException {
//        super(f);
//    }

    public ResultClusterEventLogger(String f, int cluster) throws IOException {
        super(f, cluster);
    }

    @Override
    public void onStart(long id, Iterable<String> query) {
    }

    @Override
    public void onEnd(long id, Iterable<Long> results) {
        log(id, Joiner.on(" ").join(results));
    }
}
