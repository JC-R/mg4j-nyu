package edu.nyu.tandon.csi;

import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.index.cluster.DocumentalPartitioningStrategy;
import it.unimi.di.big.mg4j.query.QueryEngine;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class CentralSampleIndex {

    protected DocumentalPartitioningStrategy csiStrategy;
    protected DocumentalPartitioningStrategy clustersStrategy;

    protected Index csi;
    protected QueryEngine engine;

    protected void constructQueryEngine() {
        // TODO
    }

    public QueryEngine getQueryEngine() {
        if (engine == null) constructQueryEngine();
        return engine;
    }



}
