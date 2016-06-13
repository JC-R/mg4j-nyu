package edu.nyu.tandon.experiments.logger;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public interface EventLogger {

    void onStart(Object ... o);
    void onEnd(Object ... o);

}
