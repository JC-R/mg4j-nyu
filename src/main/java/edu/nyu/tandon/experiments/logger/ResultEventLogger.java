package edu.nyu.tandon.experiments.logger;

import com.google.common.base.Joiner;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class ResultEventLogger extends FileEventLogger {

    private long queriesProcessed = 0;
    private String clusterHeader = ",cluster";

    @Override
    public String header() {
        return "id,results";
    }

    public ResultEventLogger(OutputStream o) throws IOException {
        super(o);
    }

    public ResultEventLogger(File f) throws IOException {
        super(f);
    }

    public ResultEventLogger(String f) throws IOException {
        super(f);
    }

    @Override
    public void onStart(Iterable<String> query) {
    }

    @Override
    public void onEnd(Iterable<Long> results) {
        log(String.format("%d,%s", queriesProcessed++,
                Joiner.on(" ").join(results)));
    }
}
