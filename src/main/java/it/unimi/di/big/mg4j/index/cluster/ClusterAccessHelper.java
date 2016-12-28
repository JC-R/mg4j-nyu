package it.unimi.di.big.mg4j.index.cluster;

import it.unimi.di.big.mg4j.index.Index;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class ClusterAccessHelper {

    public static Index[] getLocalIndices(DocumentalCluster merged) {
        return merged.localIndex;
    }

    public static Index[] getLocalIndices(DocumentalMergedCluster merged) {
        return merged.localIndex;
    }

    public static DocumentalMergedCluster getIndex(DocumentalMergedClusterIndexIterator it) {
        return it.index;
    }

}
