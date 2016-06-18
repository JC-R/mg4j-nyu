package edu.nyu.tandon.experiments;

import com.google.common.base.Joiner;
import com.martiansoftware.jsap.*;
import edu.nyu.tandon.experiments.logger.EventLogger;
import edu.nyu.tandon.experiments.logger.FileEventLogger;
import edu.nyu.tandon.experiments.logger.ResultEventLogger;
import edu.nyu.tandon.experiments.logger.TimeEventLogger;
import edu.nyu.tandon.query.Query;
import edu.nyu.tandon.shard.csi.CentralSampleIndex;
import edu.nyu.tandon.shard.ranking.ShardSelector;
import edu.nyu.tandon.shard.ranking.redde.ReDDEShardSelector;
import it.unimi.di.big.mg4j.query.nodes.QueryBuilderVisitorException;
import it.unimi.di.big.mg4j.query.parser.QueryParserException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class SelectShards {

    public static final Logger LOGGER = LoggerFactory.getLogger(SelectShards.class);

    public static void main(String[] args) throws Exception {

        SimpleJSAP jsap = new SimpleJSAP(Query.class.getName(), "Loads indices relative to a collection, possibly loads the collection, and answers to queries.",
                new Parameter[]{
                        new FlaggedOption("input", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'i', "input", "The input file with queries delimited by new lines."),
                        new FlaggedOption("timeOutput", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 't', "time-output", "The output file to store execution times."),
                        new FlaggedOption("resultOutput", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'r', "result-output", "The output file to store results."),
                        new UnflaggedOption("basename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of the cluster indices (not including number suffixes). In other words, the basename of the partitioned index as if loaded as a DocumentalMergedCluster."),
                        new UnflaggedOption("csi", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of the central sample index.")
                });

        final JSAPResult jsapResult = jsap.parse(args);
        if (jsap.messagePrinted()) return;

        LOGGER.info("Loading CSI...");
        CentralSampleIndex csi = CentralSampleIndex.loadCSI(jsapResult.getString("csi"), jsapResult.getString("basename"));
        // TODO: Allow different selectors
        ShardSelector shardSelector = new ReDDEShardSelector(csi);

        List<EventLogger> eventLoggers = new ArrayList<>();

        if (jsapResult.userSpecified("timeOutput")) {
            eventLoggers.add(new TimeEventLogger(jsapResult.getString("timeOutput")));
        }

        if (jsapResult.userSpecified("resultOutput")) {
            eventLoggers.add(new ResultEventLogger(jsapResult.getString("resultOutput")));
        }

        eventLoggers.add(new EventLogger() {
            @Override
            public void onStart(Object... o) {
                LOGGER.debug(String.format("Processing query: %s", Joiner.on(" ").join(o)));
            }

            @Override
            public void onEnd(Object... o) {

            }
        });

        try (Stream<String> lines = Files.lines(Paths.get(jsapResult.getString("input")))) {

            lines.forEach(query -> {
                try {

                    for (EventLogger l : eventLoggers) l.onStart(query);
                    List<Integer> shards = shardSelector.selectShards(query);
                    for (EventLogger l : eventLoggers) l.onEnd(shards.toArray());

                } catch (QueryParserException | QueryBuilderVisitorException | IOException e) {
                    LOGGER.error(String.format("There was an error while processing query: %s", query), e);
                }
            });

        }
    }

}
