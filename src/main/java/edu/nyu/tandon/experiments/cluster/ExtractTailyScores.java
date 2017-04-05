package edu.nyu.tandon.experiments.cluster;

import com.martiansoftware.jsap.*;
import edu.nyu.tandon.query.Query;
import edu.nyu.tandon.shard.ranking.ShardSelector;
import edu.nyu.tandon.shard.ranking.taily.TailyShardSelector;
import it.unimi.di.big.mg4j.query.nodes.QueryBuilderVisitorException;
import it.unimi.di.big.mg4j.query.parser.QueryParserException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Map;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class ExtractTailyScores {

    public static final Logger LOGGER = LoggerFactory.getLogger(ExtractTailyScores.class);

    public static void main(String[] args) throws Exception {

        SimpleJSAP jsap = new SimpleJSAP(Query.class.getName(), "Loads indices relative to a collection, possibly loads the collection, and answers to queries.",
                new Parameter[]{
                        new FlaggedOption("input", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'i', "input", "The input file with queries delimited by new lines."),
                        new FlaggedOption("output", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'o', "output", "The output files basename."),
                        new FlaggedOption("clusters", JSAP.INTEGER_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'c', "clusters", "The number of clusters."),
                        new UnflaggedOption("basename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of the cluster indices (not including number suffixes). In other words, the basename of the partitioned index as if loaded as a DocumentalMergedCluster."),
                });

        final JSAPResult jsapResult = jsap.parse(args);
        if (jsap.messagePrinted()) return;

        int clusters = jsapResult.getInt("clusters");
        TailyShardSelector shardSelector = new TailyShardSelector(jsapResult.getString("basename"), clusters);

        String[] filenames = new String[clusters];
        for (int i = 0; i < clusters; i++) {
            filenames[i] = jsapResult.getString("output") + "#" + i + ".taily";
        }

        ExtractShardScores.run(new File(jsapResult.getString("input")), "taily", filenames, shardSelector);

    }
}
