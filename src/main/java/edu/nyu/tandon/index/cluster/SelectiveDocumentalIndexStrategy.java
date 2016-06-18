package edu.nyu.tandon.index.cluster;

import com.martiansoftware.jsap.*;
import edu.nyu.tandon.utils.FileAsciiLongIterator;
import it.unimi.di.big.mg4j.index.cluster.DocumentalClusteringStrategy;
import it.unimi.di.big.mg4j.index.cluster.DocumentalPartitioningStrategy;
import it.unimi.dsi.fastutil.ints.IntBigArrayBigList;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongBigArrayBigList;
import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;

import static it.unimi.dsi.fastutil.io.BinIO.asLongIterator;
import static it.unimi.dsi.fastutil.io.BinIO.storeObject;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class SelectiveDocumentalIndexStrategy implements DocumentalPartitioningStrategy, DocumentalClusteringStrategy, Serializable {

    public static final String STRATEGY = ".strategy";

    public static final Logger LOGGER = LoggerFactory.getLogger(SelectiveDocumentalIndexStrategy.class);

    private static final long serialVersionUID = 0L;

    protected IntBigArrayBigList localIndices;
    protected LongBigArrayBigList localPointers;
    protected LongBigArrayBigList[] globalPointers;
    protected long[] numberOfDocuments;

    protected SelectiveDocumentalIndexStrategy(int numberOfClusters, long totalNumberOfDocuments) {

        localIndices = new IntBigArrayBigList(totalNumberOfDocuments);
        localIndices.size(totalNumberOfDocuments);

        localPointers = new LongBigArrayBigList();
        localPointers.size(totalNumberOfDocuments);

        globalPointers = new LongBigArrayBigList[numberOfClusters];
        numberOfDocuments = new long[numberOfClusters];
        for (int i = 0; i < numberOfClusters; i++) {
            globalPointers[i] = new LongBigArrayBigList();
        }
    };

    @Override
    public long globalPointer(int i, long l) {
        return globalPointers[i].getLong(l);
    }

    @Override
    public int localIndex(long l) {
        return localIndices.getInt(l);
    }

    @Override
    public long localPointer(long l) {
        return localPointers.getLong(l);
    }

    @Override
    public long numberOfDocuments(int i) {
        return numberOfDocuments[i];
    }

    @Override
    public int numberOfLocalIndices() {
        return numberOfDocuments.length;
    }

    @Override
    public Properties[] properties() {
        Properties[] properties = new Properties[numberOfDocuments.length];
        for(int i = 0; i < numberOfDocuments.length; ++i) {
            properties[i] = new Properties();
        }
        return properties;
    }

    public static SelectiveDocumentalIndexStrategy constructStrategy(String[] clusterFiles, boolean ascii,
                                                                     long totalNumberOfDocuments) throws IOException {
        int length = clusterFiles.length;
        Iterator<Long>[] clusters = ascii
                ? new FileAsciiLongIterator[length]
                : new LongIterator[length];
        for (int i = 0; i < length; i++) {
                clusters[i] = ascii
                        ? new FileAsciiLongIterator(clusterFiles[i])
                        : asLongIterator(clusterFiles[i]);
        }
        return constructStrategy(clusters, totalNumberOfDocuments);
    }

    public static SelectiveDocumentalIndexStrategy constructStrategy(Iterator<Long>[] clusters, long totalNumberOfDocuments) {
        SelectiveDocumentalIndexStrategy strategy = new SelectiveDocumentalIndexStrategy(clusters.length, totalNumberOfDocuments);

        int clusterId = 0;
        // For each cluster:
        for (Iterator<Long> cluster : clusters) {

            // Initialize a global pointers mapping
            strategy.globalPointers[clusterId] = new LongBigArrayBigList();

            long localDocumentId = 0;
            // For each local document in the cluster
            while  (cluster.hasNext()) {

                long globalDocumentId = cluster.next();

                // Store mapping from a global document ID to a cluster ID and a local document ID within the cluster
                strategy.localIndices.set(globalDocumentId, clusterId);
                strategy.localPointers.set(globalDocumentId, localDocumentId);

                // Store mapping from a cluster ID and a local document ID within the cluster to a global document ID
                strategy.globalPointers[clusterId].push(globalDocumentId);

                localDocumentId++;
            }

            // Set the number of documents in the cluster
            strategy.numberOfDocuments[clusterId] = localDocumentId;

            clusterId++;
        }

        return strategy;
    }

    public static void main(String[] args) throws JSAPException {

        SimpleJSAP jsap = new SimpleJSAP(SelectiveDocumentalIndexStrategy.class.getName(),
                "", // TODO
                new Parameter[] {
                        new Switch("asciiIds", 'a', "ascii-ids", "If present, the document IDs in the cluster specifications will be read as longs encoded in ascii delimited by new lines."),
                        new QualifiedSwitch("clusters", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'c', "clusters", "The files defining the clusters: either containing sorted list of global IDs or titles (only if -g provided).")
                            .setList(true).setListSeparator(','),
                        new FlaggedOption("numberOfDocuments", JSAP.LONG_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'n', "number-of-documents", "The number of documents of the global index."),
                        new UnflaggedOption("outputFile", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.GREEDY, "The output file where the serialized strategy will be stored.")
                });

        JSAPResult jsapResult = jsap.parse(args);
        if (jsap.messagePrinted()) return;

        String[] clustersPaths = jsapResult.getStringArray("clusters");

        LOGGER.info(String.format("Creating %s for %d clusters...",
                SelectiveDocumentalIndexStrategy.class.getName(),
                clustersPaths.length
                ));

        try {

            SelectiveDocumentalIndexStrategy strategy = constructStrategy(clustersPaths,
                    jsapResult.userSpecified("asciiIds"), jsapResult.getLong("numberOfDocuments"));
            storeObject(strategy, jsapResult.getString("outputFile"));

        } catch (IOException e) {
            // TODO
            e.printStackTrace();
        }

    }

}
