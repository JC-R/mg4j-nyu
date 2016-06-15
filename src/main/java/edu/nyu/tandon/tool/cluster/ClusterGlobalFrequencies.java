package edu.nyu.tandon.tool.cluster;

import com.martiansoftware.jsap.*;
import edu.nyu.tandon.query.Query;
import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.index.IndexReader;
import it.unimi.di.big.mg4j.index.cluster.DocumentalClusteringStrategy;
import it.unimi.di.big.mg4j.index.cluster.DocumentalMergedCluster;
import it.unimi.dsi.fastutil.io.BinIO;
import org.apache.commons.configuration.ConfigurationException;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;

import static it.unimi.di.big.mg4j.index.Index.getInstance;
import static it.unimi.dsi.fastutil.io.BinIO.loadObject;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class ClusterGlobalFrequencies {

    public static void globalFrequencies(DocumentalMergedCluster index, DocumentalClusteringStrategy strategy, int clusterId, String clusterBasename)
            throws IOException {

        try (DataOutputStream o = new DataOutputStream(new FileOutputStream(clusterBasename + ".freqglob"))) {
            for (long i = 0; i < strategy.numberOfDocuments(clusterId); i++) {
                long frequency = index.documents(strategy.globalPointer(clusterId, i)).frequency();
                o.writeLong(frequency);
            }
        }

    }

    public static void globalFrequencies(DocumentalMergedCluster index) throws IOException, ClassNotFoundException {

        String[] localIndices = index.properties.getStringArray("localindex");
        DocumentalClusteringStrategy strategy =
                (DocumentalClusteringStrategy) loadObject(index.properties.getString("strategy"));
        for (int i = 0; i < localIndices.length; i++) {
            globalFrequencies(index, strategy, i, localIndices[i]);
        }

    }

    public static void main(String[] args) throws JSAPException, IllegalAccessException, InvocationTargetException, NoSuchMethodException, IOException, InstantiationException, URISyntaxException, ConfigurationException, ClassNotFoundException {

        SimpleJSAP jsap = new SimpleJSAP(Query.class.getName(), "Produces a list of term frequencies based on a clustering strategy.",
                new Parameter[]{
                        new UnflaggedOption("index", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.GREEDY, "The merged cluster index basename.")
                });

        final JSAPResult jsapResult = jsap.parse(args);
        if (jsap.messagePrinted()) return;

        globalFrequencies((DocumentalMergedCluster) getInstance(jsapResult.getString("index")));

    }

}
