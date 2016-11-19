package edu.nyu.tandon.experiments.cluster;

import com.martiansoftware.jsap.*;
import edu.nyu.tandon.query.Query;
import edu.nyu.tandon.search.score.BM25PrunedScorer;
import edu.nyu.tandon.search.score.QueryLikelihoodScorer;
import edu.nyu.tandon.shard.csi.CentralSampleIndex;
import edu.nyu.tandon.shard.ranking.ShardSelector;
import edu.nyu.tandon.shard.ranking.redde.ReDDEShardSelector;
import edu.nyu.tandon.shard.ranking.shrkc.RankS;
import it.unimi.di.big.mg4j.query.nodes.QueryBuilderVisitorException;
import it.unimi.di.big.mg4j.query.parser.QueryParserException;
import it.unimi.di.big.mg4j.search.score.Scorer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class ExtractShardScores {

    public static final Logger LOGGER = LoggerFactory.getLogger(ExtractShardScores.class);

    private static ShardSelector resolveShardSelector(String name, CentralSampleIndex csi) {
        if ("redde".equals(name)) return new ReDDEShardSelector(csi);
        else if ("shrkc".equals(name)) return new RankS(csi, 2).withC(-1.0);
        else throw new IllegalArgumentException("You need to define a proper selector: redde, shrkc");
    }

    public static Scorer resolveScorer(String name) {
        if ("bm25".equals(name)) return new BM25PrunedScorer();
        else if ("ql".equals(name)) return new QueryLikelihoodScorer();
        else throw new IllegalArgumentException("You need to define a proper scorer: bm25, ql");
    }

    public static void main(String[] args) throws Exception {

        SimpleJSAP jsap = new SimpleJSAP(Query.class.getName(), "Loads indices relative to a collection, possibly loads the collection, and answers to queries.",
                new Parameter[]{
                        new FlaggedOption("input", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'i', "input", "The input file with queries delimited by new lines."),
                        new FlaggedOption("output", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'o', "output", "The output files basename."),
                        new FlaggedOption("clusters", JSAP.INTEGER_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'c', "clusters", "The number of clusters."),
                        new FlaggedOption("selector", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 's', "selector", "Selector type (redde or shrkc)"),
                        new FlaggedOption("scorer", JSAP.STRING_PARSER, "bm25", JSAP.NOT_REQUIRED, 'S', "scorer", "Scorer type (bm25 or ql)"),
                        new FlaggedOption("csiMaxOutput", JSAP.INTEGER_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'L', "csi-max-output", "CSI maximal number of results")
                                .setAllowMultipleDeclarations(true),
                        new UnflaggedOption("basename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of the cluster indices (not including number suffixes). In other words, the basename of the partitioned index as if loaded as a DocumentalMergedCluster."),
                        new UnflaggedOption("csi", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of the central sample index.")
                });

        final JSAPResult jsapResult = jsap.parse(args);
        if (jsap.messagePrinted()) return;

        int clusters = jsapResult.getInt("clusters");

        LOGGER.info("Loading CSI...");
        CentralSampleIndex csi = CentralSampleIndex.loadCSI(jsapResult.getString("csi"),
                jsapResult.getString("basename"), resolveScorer(jsapResult.getString("scorer")));

        int[] csiMaxOutputs;
        if (jsapResult.userSpecified("csiMaxOutput")) csiMaxOutputs = jsapResult.getIntArray("csiMaxOutput");
        else csiMaxOutputs = new int[] { 100 };

        for (int L : csiMaxOutputs) {

            LOGGER.info(String.format("Extracting shard scores for L = %d", L));
            csi.setMaxOutput(L);
            ShardSelector shardSelector = resolveShardSelector(jsapResult.getString("selector"), csi);

            FileWriter[] writers = new FileWriter[clusters];
            for (int i = 0; i < clusters; i++)
                writers[i] =
                        new FileWriter(jsapResult.getString("output") + "#" + i + "." + jsapResult.getString("selector") + "." + L);

            try (BufferedReader br = new BufferedReader(new FileReader(jsapResult.getString("input")))) {
                for (String query; (query = br.readLine()) != null; ) {
                    try {
                        Map<Integer, Double> shardScores = shardSelector.shardScores(query);
                        for (int i = 0; i < clusters; i++) {
                            writers[i]
                                    .append(shardScores.getOrDefault(i, 0.0).toString())
                                    .append("\n");
                        }
                    } catch (QueryParserException | QueryBuilderVisitorException | IOException e) {
                        LOGGER.error(String.format("There was an error while processing query: %s", query), e);
                        throw e;
                    }
                }
            } finally {
                for (int i = 0; i < clusters; i++)
                    try {
                        writers[i].close();
                    } catch (IOException e) {
                        LOGGER.error(String.format("Couldn't close writer for shard %d", i));
                    }
            }
        }
    }

}
