package edu.nyu.tandon.experiments.logger;

import com.google.common.base.Joiner;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class ResultEventLogger extends FileEventLogger {

    public ResultEventLogger(OutputStream o) {
        super(o);
    }

    public ResultEventLogger(File f) throws IOException {
        super(f);
    }

    public ResultEventLogger(String f) throws IOException {
        super(f);
    }

    @Override
    public void onStart(Object... o) {}

    @Override
    public void onEnd(Object... o) {
        log(Joiner.on(" ").join(o));
    }
}
