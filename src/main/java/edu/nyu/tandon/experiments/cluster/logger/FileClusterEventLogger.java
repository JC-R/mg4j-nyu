package edu.nyu.tandon.experiments.cluster.logger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public abstract class FileClusterEventLogger implements EventLogger {

    public static final Logger LOGGER = LoggerFactory.getLogger(FileClusterEventLogger.class);

    private BufferedWriter writer;

    public abstract String column();

    private void init(BufferedWriter writer) throws IOException {
        this.writer = writer;
        this.writer.append("id,")
                .append(column())
                .append('\n')
                .flush();
    }

    public FileClusterEventLogger(String f) throws IOException {
        init(new BufferedWriter(new FileWriter(f)));
    }

    protected void log(long id, String columnValue) {
        try {
            writer
                    .append(String.valueOf(id))
                    .append(',')
                    .append(columnValue)
                    .append('\n')
                    .flush();
        } catch (IOException e) {
            LOGGER.error("Writing event has failed", e);
        }
    }

}
