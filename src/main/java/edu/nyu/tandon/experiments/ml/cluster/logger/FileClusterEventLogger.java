package edu.nyu.tandon.experiments.ml.cluster.logger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public abstract class FileClusterEventLogger implements EventLogger {

    public static final Logger LOGGER = LoggerFactory.getLogger(FileClusterEventLogger.class);

    private BufferedWriter writer;

    public FileClusterEventLogger(String f) throws IOException {
        init(new BufferedWriter(new FileWriter(f)));
    }

    public abstract String column();

    private void init(BufferedWriter writer) throws IOException {
        this.writer = writer;
        this.writer.append("id,cluster,")
                .append(column())
                .append('\n')
                .flush();
    }

    protected void log(long id, int cluster, String columnValue) {
        try {
            writer
                    .append(String.valueOf(id))
                    .append(',')
                    .append(String.valueOf(cluster))
                    .append(',')
                    .append(columnValue)
                    .append('\n')
                    .flush();
        } catch (IOException e) {
            LOGGER.error("Writing event has failed", e);
        }
    }

}
