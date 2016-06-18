package edu.nyu.tandon.experiments;

import com.martiansoftware.jsap.*;
import edu.nyu.tandon.experiments.logger.EventLogger;
import edu.nyu.tandon.experiments.logger.ResultEventLogger;
import edu.nyu.tandon.experiments.logger.TimeEventLogger;
import edu.nyu.tandon.query.Query;
import edu.nyu.tandon.search.score.BM25PrunedScorer;
import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.index.TermProcessor;
import it.unimi.di.big.mg4j.query.QueryEngine;
import it.unimi.di.big.mg4j.query.SelectedInterval;
import it.unimi.di.big.mg4j.query.nodes.QueryBuilderVisitorException;
import it.unimi.di.big.mg4j.query.parser.QueryParserException;
import it.unimi.di.big.mg4j.query.parser.SimpleParser;
import it.unimi.di.big.mg4j.search.DocumentIteratorBuilderVisitor;
import it.unimi.di.big.mg4j.search.score.DocumentScoreInfo;
import it.unimi.dsi.fastutil.Hash;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.objects.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static edu.nyu.tandon.query.Query.MAX_STEMMING;
import static edu.nyu.tandon.tool.cluster.ClusterGlobalStatistics.GLOB_FREQ_EXTENSION;
import static edu.nyu.tandon.tool.cluster.ClusterGlobalStatistics.GLOB_STAT_EXTENSION;
import static it.unimi.dsi.fastutil.io.BinIO.loadLongs;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class RunQueries {

    public static final Logger LOGGER = LoggerFactory.getLogger(RunQueries.class);

    public static void main(String[] args) throws Exception {

        SimpleJSAP jsap = new SimpleJSAP(Query.class.getName(), "Loads indices relative to a collection, possibly loads the collection, and answers to queries.",
                new Parameter[]{
                        new Switch("globalStatistics", 'g', "global-statistics", "Whether to use global statistics. Note that they need to be calculated: see ClusterGlobalStatistics."),
                        new FlaggedOption("input", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'i', "input", "The input file with queries delimited by new lines."),
                        new FlaggedOption("timeOutput", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 't', "time-output", "The output file to store execution times."),
                        new FlaggedOption("resultOutput", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'r', "result-output", "The output file to store results."),
                        new UnflaggedOption("basename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of the index.")
                });

        final JSAPResult jsapResult = jsap.parse(args);
        if (jsap.messagePrinted()) return;

        String basename = jsapResult.getString("basename");
        String[] basenameWeight = new String[] { basename };

        final Object2ReferenceLinkedOpenHashMap<String, Index> indexMap = new Object2ReferenceLinkedOpenHashMap<String, Index>(Hash.DEFAULT_INITIAL_SIZE, .5f);
        final Reference2DoubleOpenHashMap<Index> index2Weight = new Reference2DoubleOpenHashMap<Index>();
        Query.loadIndicesFromSpec(basenameWeight, true, null, indexMap, index2Weight);

        final Object2ObjectOpenHashMap<String, TermProcessor> termProcessors = new Object2ObjectOpenHashMap<String, TermProcessor>(indexMap.size());
        for (String alias : indexMap.keySet()) termProcessors.put(alias, indexMap.get(alias).termProcessor);
        final SimpleParser simpleParser = new SimpleParser(indexMap.keySet(), indexMap.firstKey(), termProcessors);
        final Reference2ReferenceMap<Index, Object> index2Parser = new Reference2ReferenceOpenHashMap<Index, Object>();

        QueryEngine engine = new QueryEngine(
                simpleParser,
                new DocumentIteratorBuilderVisitor(indexMap, index2Parser, indexMap.get(indexMap.firstKey()), MAX_STEMMING),
                indexMap);
        engine.setWeights(index2Weight);
        BM25PrunedScorer scorer = new BM25PrunedScorer();
        if (jsapResult.userSpecified("globalStatistics")) {
            LOGGER.info("Running queries with global statistics.");
            LongArrayList frequencies = new LongArrayList(loadLongs(basename + GLOB_FREQ_EXTENSION));
            long[] globalStats = loadLongs(basename + GLOB_STAT_EXTENSION);
            if (globalStats.length != 2) {
                throw new IllegalStateException(String.format("File %s must contain 2 longs (but contains %d)",
                        basename + GLOB_STAT_EXTENSION, globalStats.length));
            }
            scorer.setGlobalMetrics(globalStats[0], globalStats[1], frequencies);
        }
        engine.score(scorer);

        List<EventLogger> eventLoggers = new ArrayList<>();

        if (jsapResult.userSpecified("timeOutput")) {
            eventLoggers.add(new TimeEventLogger(jsapResult.getString("timeOutput")));
        }

        if (jsapResult.userSpecified("resultOutput")) {
            eventLoggers.add(new ResultEventLogger(jsapResult.getString("resultOutput")));
        }

        try (Stream<String> lines = Files.lines(Paths.get(jsapResult.getString("input")))) {

            lines.forEach(query -> {
                try {

                    for (EventLogger l : eventLoggers) l.onStart(query);
                    ObjectArrayList<DocumentScoreInfo<Reference2ObjectMap<Index, SelectedInterval[]>>> r =
                            new ObjectArrayList<>();
                    int docs = engine.process(query, 0, 10, r);
                    for (EventLogger l : eventLoggers) l.onEnd(r.stream().map(dsi -> Long.valueOf(dsi.document)).toArray());

                } catch (QueryParserException | QueryBuilderVisitorException | IOException e) {
                    LOGGER.error(String.format("There was an error while processing query: %s", query), e);
                }
            });

        }

    }

}
