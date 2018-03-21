package edu.nyu.tandon.experiments.l2rr;

import com.martiansoftware.jsap.*;
import edu.nyu.tandon.query.Query;
import edu.nyu.tandon.utils.Utils;
import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.index.IndexReader;
import it.unimi.di.big.mg4j.index.cluster.ClusterAccessHelper;
import it.unimi.di.big.mg4j.index.cluster.DocumentalMergedCluster;
import it.unimi.di.big.mg4j.search.AndDocumentIterator;
import it.unimi.di.big.mg4j.search.DocumentIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

import static it.unimi.di.big.mg4j.search.DocumentIterator.END_OF_LIST;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class ExtractBigramFrequency {

    public static final Logger LOGGER = LoggerFactory.getLogger(ExtractBigramFrequency.class);

    public static void processQuery(DocumentalMergedCluster index, String query, int queryIdx) throws IOException {
        Index[] shards = ClusterAccessHelper.getLocalIndices(index);
        int shardIdx = 0;
        for (Index shard : shards) {
            try (IndexReader shardReader = shard.getReader()) {
                List<String> terms = Utils.extractTerms(query, shard.termProcessor);
                double bigramLogFrequency = 0.0;
                for (int i = 0; i < terms.size(); i++) {
                    for (int j = i; j < terms.size(); j++) {
                        DocumentIterator intersection = AndDocumentIterator.getInstance(
                                shardReader.documents((terms.get(i))),
                                shardReader.documents((terms.get(j))));
                        long frequency = 0;
                        while (intersection.nextDocument() != END_OF_LIST) {
                            frequency++;
                        }
                        bigramLogFrequency += Math.log(1 + frequency);
                    }
                }
                StringBuilder line = new StringBuilder();
                line.append(queryIdx)
                        .append(',').append(shardIdx)
                        .append(',').append(bigramLogFrequency);
                System.out.println(line);
                shardIdx++;
            }
        }
    }

    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws Exception {

        SimpleJSAP jsap = new SimpleJSAP(Query.class.getName(), ".",
                new Parameter[]{
                        new Switch("skipHeader", 'H', "skip-header", "Do not write CSV header."),
                        new FlaggedOption("input", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'i', "input", "The input file with queries delimited by new lines."),
                        new UnflaggedOption("basename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of the index."),
                });

        final JSAPResult jsapResult = jsap.parse(args);
        if (jsap.messagePrinted()) return;

        String basename = jsapResult.getString("basename");
        String inputFile = jsapResult.getString("input");

        DocumentalMergedCluster index = (DocumentalMergedCluster) Index.getInstance(basename, true, true, true);

        try (BufferedReader queryReader = new BufferedReader(new FileReader(inputFile))) {
            if (!jsapResult.userSpecified("skipHeader")) {
                System.out.println("query,shard,bilogfq");
            }
            String query;
            int queryIdx = 0;
            while ((query = queryReader.readLine()) != null) {
                processQuery(index, query, queryIdx++);
            }
        }
    }

}
