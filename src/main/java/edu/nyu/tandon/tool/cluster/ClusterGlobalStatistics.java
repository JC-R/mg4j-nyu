package edu.nyu.tandon.tool.cluster;

import com.martiansoftware.jsap.*;
import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.index.IndexAccessHelper;
import it.unimi.di.big.mg4j.index.IndexIterator;
import it.unimi.di.big.mg4j.index.IndexReader;
import it.unimi.di.big.mg4j.index.cluster.ClusterAccessHelper;
import it.unimi.di.big.mg4j.index.cluster.DocumentalCluster;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongBigArrayBigList;
import org.apache.commons.configuration.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;

import static it.unimi.di.big.mg4j.index.Index.getInstance;
import static it.unimi.dsi.fastutil.io.BinIO.loadLongs;
import static it.unimi.dsi.fastutil.io.BinIO.loadLongsBig;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class ClusterGlobalStatistics {

    public static final Logger LOGGER = LoggerFactory.getLogger(ClusterGlobalStatistics.class);

    public static final String GLOB_FREQ_EXTENSION = ".globfreq";
    public static final String GLOB_OCC_EXTENSION = ".globocc";
    public static final String GLOB_STAT_EXTENSION = ".globstat";

    public static LongArrayList loadGlobalFrequencies(String basename) throws IOException {
        return new LongArrayList(loadLongs(basename + GLOB_FREQ_EXTENSION));
    }

    public static LongBigArrayBigList loadGlobalOccurrencies(String basename) throws IOException {
        return new LongBigArrayBigList(loadLongsBig(basename + GLOB_OCC_EXTENSION));
    }

    public static long[] loadGlobalStats(String basename) throws IOException {
        long[] globalStats = loadLongs(basename + GLOB_STAT_EXTENSION);
        if (globalStats.length != 2) {
            throw new IllegalStateException(String.format("File %s must contain 2 longs (but contains %d)",
                    basename + GLOB_STAT_EXTENSION, globalStats.length));
        }
        return globalStats;
    }

    public static long frequency(Index index, long term) throws IOException {
        return index.documents(term).frequency();
    }

    public static void globalOccurrencies(Index index, String clusterBasename)
            throws IOException, IllegalAccessException, InvocationTargetException, InstantiationException, NoSuchMethodException, URISyntaxException, ConfigurationException, ClassNotFoundException {

        try (DataOutputStream o = new DataOutputStream(new FileOutputStream(clusterBasename + GLOB_OCC_EXTENSION));
             IndexReader indexReader = index.getReader()) {
            IndexIterator indexIterator;
            while ((indexIterator = indexReader.nextIterator()) != null) {
                o.writeLong(IndexAccessHelper.getOccurrency(indexIterator));
            }
        }

    }

    public static void globalFrequencies(Index index, String clusterBasename)
            throws IOException, IllegalAccessException, InvocationTargetException, InstantiationException, NoSuchMethodException, URISyntaxException, ConfigurationException, ClassNotFoundException {

        try (DataOutputStream o = new DataOutputStream(new FileOutputStream(clusterBasename + GLOB_FREQ_EXTENSION));
             IndexReader indexReader = index.getReader()) {
            IndexIterator indexIterator;
            while ((indexIterator = indexReader.nextIterator()) != null) {
                o.writeLong(indexIterator.frequency());
            }
        }

    }

    public static void globalStats(Index index, String clusterBasename)
            throws IOException {

        try (DataOutputStream o = new DataOutputStream(new FileOutputStream(clusterBasename + GLOB_STAT_EXTENSION))) {
            o.writeLong(index.numberOfDocuments);
            o.writeLong(index.numberOfOccurrences);
        }

    }

    public static void globalStatistics(DocumentalCluster index, boolean skipOccurrencies)
            throws IOException, ClassNotFoundException, IllegalAccessException, URISyntaxException, InstantiationException, ConfigurationException, NoSuchMethodException, InvocationTargetException {

        String[] localBasenames = index.properties.getStringArray("localindex");
        Index[] localIndices = ClusterAccessHelper.getLocalIndices(index);
        //for (Index localIndex : ClusterAccessHelper.getLocalIndices(index)) {
        for (int i = 0; i < localIndices.length; i++) {
            LOGGER.info(String.format("Creating global statistics for cluster %s", localBasenames[i]));
            globalStats(localIndices[i], localBasenames[i]);
            globalFrequencies(localIndices[i], localBasenames[i]);
            if (!skipOccurrencies) globalOccurrencies(localIndices[i], localBasenames[i]);
        }

    }

    public static void main(String[] args) throws JSAPException, IllegalAccessException, InvocationTargetException, NoSuchMethodException, IOException, InstantiationException, URISyntaxException, ConfigurationException, ClassNotFoundException {

        SimpleJSAP jsap = new SimpleJSAP(ClusterGlobalStatistics.class.getName(), "Produces a list of term frequencies [and occurrencies] based on a clustering strategy.",
                new Parameter[]{
                        new Switch("skipOccurrencies", 's', "skip-occurrencies", "Do not produce the list of global occurrencies."),
                        new UnflaggedOption("index", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.GREEDY, "The documental merged index for which we want to compute global frequencies.")
                });

        final JSAPResult jsapResult = jsap.parse(args);
        if (jsap.messagePrinted()) return;

        String basename = jsapResult.getString("index");
        DocumentalCluster index = (DocumentalCluster) getInstance(basename);
        LOGGER.info(String.format("Creating global statistics for clusters in %s", basename));
        globalStatistics(index, jsapResult.userSpecified("skipOccurrencies"));

    }

}
