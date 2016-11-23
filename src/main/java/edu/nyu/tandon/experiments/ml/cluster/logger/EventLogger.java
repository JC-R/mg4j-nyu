package edu.nyu.tandon.experiments.ml.cluster.logger;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public interface EventLogger {

    void onStart(long id, int cluster, Iterable<String> query);
    void onEnd(long id, int cluster, Iterable<Long> results);

}
