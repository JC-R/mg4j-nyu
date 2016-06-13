package edu.nyu.tandon.experiments.querylistener;

import com.google.common.base.Joiner;
import it.unimi.di.big.mg4j.search.score.DocumentScoreInfo;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class TimeQueryLogger<T> implements QueryListener<T> {

    public static final Logger LOGGER = LoggerFactory.getLogger(TimeQueryLogger.class);

    private long timestamp;
    private BufferedWriter writer;

    public TimeQueryLogger(OutputStream outputStream) {
        writer = new BufferedWriter(new OutputStreamWriter(outputStream));
    }

    @Override
    public void onQueryStart(String query) {
        timestamp = System.currentTimeMillis();
    }

    @Override
    public void onQueryEnd(String query, ObjectArrayList<DocumentScoreInfo<T>> results) {
        try {
            writer.append(String.valueOf(System.currentTimeMillis() - timestamp));
            writer.newLine();
        } catch (IOException e) {
            LOGGER.error("Writing query execution time has failed", e);
        }
    }
}
