package edu.nyu.tandon.tool.cluster;

import com.martiansoftware.jsap.*;
import edu.nyu.tandon.query.Query;
import it.unimi.di.big.mg4j.index.DiskBasedIndex;
import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.index.IndexReader;
import it.unimi.di.big.mg4j.index.cluster.DocumentalClusteringStrategy;
import it.unimi.di.big.mg4j.index.cluster.DocumentalMergedCluster;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;

import static it.unimi.di.big.mg4j.index.DiskBasedIndex.TERMS_EXTENSION;
import static it.unimi.di.big.mg4j.index.Index.getInstance;
import static it.unimi.dsi.fastutil.io.BinIO.loadObject;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class ClusterGlobalStatistics {

    public static final String GLOB_FREQ_EXTENSION = ".globfreq";
    public static final String GLOB_STAT_EXTENSION = ".globstat";

    public static long frequency(Index index, long term) throws IOException {
        return index.documents(term).frequency();
    }

    public static void globalFrequencies(Index index, int clusterId, String clusterBasename)
            throws IOException, IllegalAccessException, InvocationTargetException, InstantiationException, NoSuchMethodException, URISyntaxException, ConfigurationException, ClassNotFoundException {

        Index cluster = getInstance(clusterBasename);

        try (DataOutputStream o = new DataOutputStream(new FileOutputStream(clusterBasename + GLOB_FREQ_EXTENSION));
             IndexReader indexReader = index.getReader()) {

            LineIterator it = FileUtils.lineIterator(new File(clusterBasename + TERMS_EXTENSION));
            while (it.hasNext()) {
                String term = it.nextLine();
                o.writeLong(indexReader.documents(term).frequency());
            }
            it.close();
        }

    }

    public static void globalStats(Index index, String clusterBasename)
            throws IOException {

        try (DataOutputStream o = new DataOutputStream(new FileOutputStream(clusterBasename + GLOB_STAT_EXTENSION))) {
            o.writeLong(index.numberOfDocuments);
            o.writeLong(index.numberOfOccurrences);
        }

    }

    public static void globalStatistics(DocumentalMergedCluster index, Index globalIndex)
            throws IOException, ClassNotFoundException, IllegalAccessException, URISyntaxException, InstantiationException, ConfigurationException, NoSuchMethodException, InvocationTargetException {

        String[] localIndices = index.properties.getStringArray("localindex");
        for (int i = 0; i < localIndices.length; i++) {
            globalFrequencies(globalIndex, i, localIndices[i]);
            globalStats(globalIndex, localIndices[i]);
        }

    }

    public static void main(String[] args) throws JSAPException, IllegalAccessException, InvocationTargetException, NoSuchMethodException, IOException, InstantiationException, URISyntaxException, ConfigurationException, ClassNotFoundException {

        SimpleJSAP jsap = new SimpleJSAP(Query.class.getName(), "Produces a list of term frequencies based on a clustering strategy.",
                new Parameter[]{
                        new UnflaggedOption("index", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.GREEDY, "The docuental merged index for which we want to compute global frequencies."),
                        new FlaggedOption("globalIndex", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'g', "global-index", "The index with global statistics. If provided, it will be used instead of <index> to compute global statistics.")
                });

        final JSAPResult jsapResult = jsap.parse(args);
        if (jsap.messagePrinted()) return;

        DocumentalMergedCluster index = (DocumentalMergedCluster) getInstance(jsapResult.getString("index"));
        globalStatistics(index,
                jsapResult.userSpecified("globalIndex")
                        ? (Index) getInstance(jsapResult.getString("globalIndex"))
                        : index
        );

    }

}
