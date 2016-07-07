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

    public abstract String header();

    public void init(BufferedWriter writer) throws IOException {
        this.writer = writer;
        this.writer.append(header() + "\n");
    }

    public FileEventLogger(OutputStream o) throws IOException {
        init(new BufferedWriter(new OutputStreamWriter(o)));
    }

    public FileEventLogger(File f) throws IOException {
        init(new BufferedWriter(new FileWriter(f)));
    }

    public FileEventLogger(String f) throws IOException {
        init(new BufferedWriter(new FileWriter(f)));
    }

    protected void log(String s) {
        try {
            writer.append(s + "\n")
                    .flush();
        } catch (IOException e) {
            LOGGER.error("Writing event has failed", e);
        }
    }

}
