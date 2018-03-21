package edu.nyu.tandon.experiments.l2rr;

import com.martiansoftware.jsap.*;
import edu.nyu.tandon.query.Query;
import edu.nyu.tandon.utils.Utils;
import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.index.cluster.ClusterAccessHelper;
import it.unimi.di.big.mg4j.index.cluster.DocumentalMergedCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class ExtractQueryTermStatistics {

    public static final Logger LOGGER = LoggerFactory.getLogger(ExtractQueryTermStatistics.class);

    public static void processQuery(DocumentalMergedCluster index, String query, int queryIdx) throws IOException {
        Index[] shards = ClusterAccessHelper.getLocalIndices(index);
        int shardIdx = 0;
        for (Index shard : shards) {
            List<String> terms = Utils.extractTerms(query, shard.termProcessor);
            long maxstf = Long.MIN_VALUE;
            long minstf = Long.MAX_VALUE;
            double maxstfidf = Double.MIN_VALUE;
            double minstfidf = Double.MAX_VALUE;
            for (String term : terms) {
                long stf = shard.documents(term).frequency();
                double idf = Math.log((double) index.numberOfDocuments
                        / (double) (index.documents(term).frequency() + 1));
                double stfidf = (double) stf * idf;
                maxstf = Math.max(maxstf, stf);
                minstf = Math.min(minstf, stf);
                maxstfidf = Math.max(maxstfidf, stfidf);
                minstfidf = Math.min(minstfidf, stfidf);
            }
            StringBuilder line = new StringBuilder();
            line.append(queryIdx)
                    .append(',').append(shardIdx)
                    .append(',').append(minstf)
                    .append(',').append(maxstf)
                    .append(',').append(minstfidf)
                    .append(',').append(maxstfidf);
            System.out.println(line);
            shardIdx++;
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
                System.out.println("query,shard,minstf,maxstf,minstfidf,maxstfidf");
            }
            String query;
            int queryIdx = 0;
            while ((query = queryReader.readLine()) != null) {
                processQuery(index, query, queryIdx++);
            }
        }
    }

}
