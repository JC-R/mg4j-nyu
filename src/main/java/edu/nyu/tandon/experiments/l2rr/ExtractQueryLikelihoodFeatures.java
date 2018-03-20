package edu.nyu.tandon.experiments.l2rr;

import com.martiansoftware.jsap.*;
import edu.nyu.tandon.query.Query;
import edu.nyu.tandon.utils.Utils;
import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.index.IndexAccessHelper;
import it.unimi.di.big.mg4j.index.IndexIterator;
import it.unimi.di.big.mg4j.index.IndexReader;
import it.unimi.di.big.mg4j.index.cluster.ClusterAccessHelper;
import it.unimi.di.big.mg4j.index.cluster.DocumentalMergedCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static it.unimi.di.big.mg4j.search.DocumentIterator.END_OF_LIST;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class ExtractQueryLikelihoodFeatures {

    public static final Logger LOGGER = LoggerFactory.getLogger(ExtractQueryLikelihoodFeatures.class);
    public static final double TINY_PROBABILITY = 0.00000000000000001;
    public static final double TINY_LOG_PROBABILITY = Math.log(TINY_PROBABILITY);

    public static double processShard(Index shard, String query, double lambda) throws IOException {
        List<String> terms = Utils.extractTerms(query, shard.termProcessor);
        double queryLikelihood = 0.0;
        try (IndexReader reader = shard.getReader()) {
            for (String term : terms) {
                IndexIterator iterator = reader.documents(term);

                double occurrencesInCollection = (double) IndexAccessHelper.getOccurrency(iterator);
                double collectionSize = (double) shard.numberOfOccurrences;
                double outsideListDocCount = (double) (shard.numberOfDocuments - iterator.frequency());
                double lambdaCollectionProbability = lambda * occurrencesInCollection / collectionSize;
                double outsideListComponent = outsideListDocCount * lambdaCollectionProbability;

                double inListComponent = 0.0;
                long doc;
                while ((doc = iterator.nextDocument()) != END_OF_LIST) {
                    inListComponent += lambdaCollectionProbability;
                    inListComponent += (1.0 - lambda) * (double) iterator.count() / (double) shard.sizes.getInt(doc);
                }
                double numerator = outsideListComponent + inListComponent;

                double termShardLogModel = TINY_LOG_PROBABILITY;
                if (numerator > 0) {
                    termShardLogModel = Math.log(numerator) - Math.log(shard.numberOfDocuments);
                }
                queryLikelihood += termShardLogModel;
            }
        }
        return queryLikelihood;
    }

    public static void processQuery(Index[] shards, String query, int queryIdx, double lambda, int binSize) throws IOException {
        QueryLikelihood[] queryLikelihoods = new QueryLikelihood[shards.length];
        int shardIdx = 0;
        for (Index shard : shards) {
            queryLikelihoods[shardIdx] = new QueryLikelihood(shardIdx, processShard(shard, query, lambda));
            shardIdx++;
        }
        Arrays.sort(queryLikelihoods, (QueryLikelihood lhs, QueryLikelihood rhs) ->
                -Double.compare(lhs.queryLikelihood, rhs.queryLikelihood)
        );
        for (int binStart = 0; binStart < shards.length; binStart += binSize) {
            int binEnd = Math.min(binStart + binSize, shards.length);
            double binnedQL = queryLikelihoods[binStart].queryLikelihood;
            for (int idx = binStart; idx < binEnd; ++idx) {
                queryLikelihoods[idx].binnedQueryLikelihood = binnedQL;
            }
        }
        Arrays.sort(queryLikelihoods, (QueryLikelihood lhs, QueryLikelihood rhs) ->
                Integer.compare(lhs.shard, rhs.shard)
        );
        for (QueryLikelihood ql : queryLikelihoods) {
            StringBuilder line = new StringBuilder();
            line.append(queryIdx)
                    .append(',').append(ql.shard)
                    .append(',').append(ql.queryLikelihood)
                    .append(',').append(ql.invQueryLikelihood)
                    .append(',').append(ql.binnedQueryLikelihood);
            System.out.println(line);
        }
    }

    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws Exception {

        SimpleJSAP jsap = new SimpleJSAP(Query.class.getName(), ".",
                new Parameter[]{
                        new Switch("skipHeader", 'H', "skip-header", "Do not write CSV header."),
                        new FlaggedOption("input", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'i', "input", "The input file with queries delimited by new lines."),
                        new FlaggedOption("lambda", JSAP.DOUBLE_PARSER, "0.4", JSAP.REQUIRED, 'l', "lambda", "Lambda in Jelinek-Mercer smoothing."),
                        new FlaggedOption("binSize", JSAP.INTEGER_PARSER, "10", JSAP.REQUIRED, 'b', "bin-size", "Bin size for binned QL."),
                        new UnflaggedOption("basename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of the index."),
                });

        final JSAPResult jsapResult = jsap.parse(args);
        if (jsap.messagePrinted()) return;

        String basename = jsapResult.getString("basename");
        String inputFile = jsapResult.getString("input");
        double lambda = jsapResult.getDouble("lambda");
        int binSize = jsapResult.getInt("binSize");

        DocumentalMergedCluster index = (DocumentalMergedCluster) Index.getInstance(basename, true, true, true);
        Index[] shards = ClusterAccessHelper.getLocalIndices(index);

        try (BufferedReader queryReader = new BufferedReader(new FileReader(inputFile))) {
            if (!jsapResult.userSpecified("skipHeader")) {
                System.out.println("query,shard,ql,invql,binql");
            }
            String query;
            int queryIdx = 0;
            while ((query = queryReader.readLine()) != null) {
                processQuery(shards, query, queryIdx++, lambda, binSize);
            }
        }
    }

    public static class QueryLikelihood {
        public int shard;
        public double queryLikelihood;
        public double invQueryLikelihood;
        public double binnedQueryLikelihood;

        public QueryLikelihood(int shard, double ql) {
            this.shard = shard;
            this.queryLikelihood = ql;
            this.invQueryLikelihood = 1 / ql;
        }
    }

}
