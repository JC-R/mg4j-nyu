package edu.nyu.tandon.experiments.cluster.logger;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public interface EventLogger {

    void onStart(long id, Iterable<String> query);
    void onEnd(long id, Iterable<Object> results);

}
