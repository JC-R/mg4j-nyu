package edu.nyu.tandon.index.cluster;

import it.unimi.di.big.mg4j.index.cluster.DocumentalPartitioningStrategy;
import it.unimi.dsi.util.Properties;

public class DocumentalStrategies {

    protected DocumentalStrategies() {
    }

    /* juan
     *
     * creates a pruned documental strategy
     */
    public static DocumentalPartitioningStrategy pruned() {

        return new DocumentalPartitioningStrategy() {

            @Override
            public int localIndex(long globalPointer) {
                return 0;
            }

            @Override
            public long localPointer(long globalPointer) {
                return 0;
            }

            @Override
            public long numberOfDocuments(int localIndex) {
                return 0;
            }

            @Override
            public int numberOfLocalIndices() {
                return 2;
            }

            @Override
            public Properties[] properties() {
                return new Properties[0];
            }
        };
    }
}
