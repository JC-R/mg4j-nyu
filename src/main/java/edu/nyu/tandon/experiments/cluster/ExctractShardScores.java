package edu.nyu.tandon.experiments.cluster;

import com.google.common.base.Splitter;
import com.martiansoftware.jsap.*;
import edu.nyu.tandon.experiments.cluster.logger.EventLogger;
import edu.nyu.tandon.experiments.cluster.logger.TimeClusterEventLogger;
import edu.nyu.tandon.query.Query;
import edu.nyu.tandon.shard.csi.CentralSampleIndex;
import edu.nyu.tandon.shard.ranking.ShardSelector;
import edu.nyu.tandon.shard.ranking.redde.ReDDEShardSelector;
import it.unimi.di.big.mg4j.query.nodes.QueryBuilderVisitorException;
import it.unimi.di.big.mg4j.query.parser.QueryParserException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class ExctractShardScores {

    public static final Logger LOGGER = LoggerFactory.getLogger(ExctractShardScores.class);

    public static void main(String[] args) throws Exception {

        SimpleJSAP jsap = new SimpleJSAP(Query.class.getName(), "Loads indices relative to a collection, possibly loads the collection, and answers to queries.",
                new Parameter[]{
                        new FlaggedOption("input", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'i', "input", "The input file with queries delimited by new lines."),
                        new FlaggedOption("timeOutput", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 't', "time-output", "The output file to store execution times."),
                        new FlaggedOption("resultOutput", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'r', "result-output", "The output file to store results."),
//                        new FlaggedOption("reddeT", JSAP.INTEGER_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'T', "redde-t", "T parameter in ReDDE (how many shards to choose). T=10 by default."),
                        new FlaggedOption("clusters", JSAP.INTEGER_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'c', "clusters", "The number of clusters."),
                        new FlaggedOption("selector", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 's', "selector", "Selector type (ReDDE or ."),
                        new UnflaggedOption("basename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of the cluster indices (not including number suffixes). In other words, the basename of the partitioned index as if loaded as a DocumentalMergedCluster."),
                        new UnflaggedOption("csi", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of the central sample index.")
                });

        final JSAPResult jsapResult = jsap.parse(args);
        if (jsap.messagePrinted()) return;

        int clusters = jsapResult.getInt("clusters");

        LOGGER.info("Loading CSI...");
        CentralSampleIndex csi = CentralSampleIndex.loadCSI(jsapResult.getString("csi"), jsapResult.getString("basename"));
        // TODO: Allow different selectors
        ShardSelector shardSelector = new ReDDEShardSelector(csi)
                .withT(clusters);
//                .withT(jsapResult.userSpecified("reddeT") ? jsapResult.getInt("reddeT") : 10);

        List<EventLogger> eventLoggers = new ArrayList<>();

        if (jsapResult.userSpecified("timeOutput")) {
            eventLoggers.add(new TimeClusterEventLogger(jsapResult.getString("timeOutput")));
        }

//        if (jsapResult.userSpecified("resultOutput")) {
//            eventLoggers.add(new ResultClusterEventLogger(jsapResult.getString("resultOutput"), cluster));
//        }

        try (BufferedReader br = new BufferedReader(new FileReader(jsapResult.getString("input")))) {
            long id = 0;
            for (String query; (query = br.readLine()) != null; ) {
                try {

                    for (EventLogger l : eventLoggers) {
                        for (int i = 0; i < clusters; i++) l.onStart(id, i, Splitter.on(" ").split(query));
                    }
                    List<Integer> shards = shardSelector.selectShards(query);
                    for (EventLogger l : eventLoggers) {
                        for (int i = 0; i < clusters; i++) {
                            l.onEnd(id, i, shards.stream().map(s -> s.longValue()).collect(Collectors.toList()));
                        }
                    }

                } catch (QueryParserException | QueryBuilderVisitorException | IOException e) {
                    LOGGER.error(String.format("There was an error while processing query: %s", query), e);
                } finally {
                    id++;
                }
            }
        }
    }

}
