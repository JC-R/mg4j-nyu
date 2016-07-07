package edu.nyu.tandon.experiments.logger;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public interface EventLogger {

    void onStart(Iterable<String> query);
    void onEnd(Iterable<Long> results);

}
