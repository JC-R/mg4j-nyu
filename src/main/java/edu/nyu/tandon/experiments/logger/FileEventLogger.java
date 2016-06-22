package edu.nyu.tandon.experiments.logger;

import com.google.common.base.Joiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public abstract class FileEventLogger implements EventLogger {

    public static final Logger LOGGER = LoggerFactory.getLogger(FileEventLogger.class);

    private BufferedWriter writer;

    public FileEventLogger(OutputStream o) {
        writer = new BufferedWriter(new OutputStreamWriter(o));
    }

    public FileEventLogger(File f) throws IOException {
        writer = new BufferedWriter(new FileWriter(f));
    }

    public FileEventLogger(String f) throws IOException {
        writer = new BufferedWriter(new FileWriter(f));
    }

    protected void log(Object ... o) {
        try {
            writer.append(Joiner.on(" ").join(o));
            writer.newLine();
            writer.flush();
        } catch (IOException e) {
            LOGGER.error("Writing event has failed", e);
        }
    }

}
