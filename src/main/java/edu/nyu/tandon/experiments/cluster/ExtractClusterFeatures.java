package edu.nyu.tandon.experiments.cluster;

import com.google.common.base.CharMatcher;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.martiansoftware.jsap.*;
import edu.nyu.tandon.experiments.cluster.logger.EventLogger;
import edu.nyu.tandon.experiments.cluster.logger.FileClusterEventLogger;
import edu.nyu.tandon.experiments.cluster.logger.ResultClusterEventLogger;
import edu.nyu.tandon.experiments.cluster.logger.TimeClusterEventLogger;
import edu.nyu.tandon.query.Query;
import edu.nyu.tandon.query.QueryEngine;
import edu.nyu.tandon.search.score.BM25PrunedScorer;
import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.index.TermProcessor;
import it.unimi.di.big.mg4j.query.SelectedInterval;
import it.unimi.di.big.mg4j.query.parser.SimpleParser;
import it.unimi.di.big.mg4j.search.DocumentIteratorBuilderVisitor;
import it.unimi.di.big.mg4j.search.score.DocumentScoreInfo;
import it.unimi.dsi.fastutil.Hash;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.objects.*;
import it.unimi.dsi.lang.MutableString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spark_project.guava.collect.Lists;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static edu.nyu.tandon.query.Query.MAX_STEMMING;
import static edu.nyu.tandon.tool.cluster.ClusterGlobalStatistics.*;
import static it.unimi.dsi.fastutil.io.BinIO.loadLongs;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class ExtractClusterFeatures {

    public static final Logger LOGGER = LoggerFactory.getLogger(ExtractClusterFeatures.class);

    // TODO: Should be more general and support other kinds of aliases.
    private static final String ALIAS = "text";

    public static void main(String[] args) throws Exception {

        SimpleJSAP jsap = new SimpleJSAP(Query.class.getName(), "Loads indices relative to a collection, possibly loads the collection, and answers to queries.",
                new Parameter[]{
                        new Switch("globalStatistics", 'g', "global-statistics", "Whether to use global statistics. Note that they need to be calculated: see ClusterGlobalStatistics."),
                        new FlaggedOption("input", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'i', "input", "The input file with queries delimited by new lines."),
                        new FlaggedOption("timeOutput", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 't', "time-output", "The output file to store execution times."),
                        new FlaggedOption("resultOutput", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'r', "result-output", "The output file to store results."),
                        new FlaggedOption("listLengthsOutput", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'l', "list-lengths-output", "The output file to store inverted list lengths."),
                        new FlaggedOption("queryLengthOutput", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'q', "query-length-output", "The output file to store query lengths."),
                        new FlaggedOption("topK", JSAP.INTEGER_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'k', "top-k", "The engine will limit the result set to top k results. k=10 by default."),
                        new UnflaggedOption("basename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of the index.")
                });

        final JSAPResult jsapResult = jsap.parse(args);
        if (jsap.messagePrinted()) return;

        String basename = jsapResult.getString("basename");
        String[] basenameWeight = new String[] { basename };

        final Object2ReferenceLinkedOpenHashMap<String, Index> indexMap = new Object2ReferenceLinkedOpenHashMap<>(Hash.DEFAULT_INITIAL_SIZE, .5f);
        final Reference2DoubleOpenHashMap<Index> index2Weight = new Reference2DoubleOpenHashMap<>();
        Query.loadIndicesFromSpec(basenameWeight, true, null, indexMap, index2Weight);

        final Object2ObjectOpenHashMap<String, TermProcessor> termProcessors = new Object2ObjectOpenHashMap<>(indexMap.size());
        for (String alias : indexMap.keySet()) termProcessors.put(alias, indexMap.get(alias).termProcessor);
        final SimpleParser simpleParser = new SimpleParser(indexMap.keySet(), indexMap.firstKey(), termProcessors);
        final Reference2ReferenceMap<Index, Object> index2Parser = new Reference2ReferenceOpenHashMap<>();

        QueryEngine engine = new QueryEngine(
                simpleParser,
                new DocumentIteratorBuilderVisitor(indexMap, index2Parser, indexMap.get(indexMap.firstKey()), MAX_STEMMING),
                indexMap);
        engine.setWeights(index2Weight);
        BM25PrunedScorer scorer = new BM25PrunedScorer();
        if (jsapResult.userSpecified("globalStatistics")) {
            LOGGER.info("Running queries with global statistics.");
            LongArrayList frequencies = loadGlobalFrequencies(basename);
            long[] globalStats = loadGlobalStats(basename);
            scorer.setGlobalMetrics(globalStats[0], globalStats[1], frequencies);
        }
        engine.score(scorer);

        int k = jsapResult.userSpecified("topK") ? jsapResult.getInt("topK") : 10;

        List<EventLogger> eventLoggers = new ArrayList<>();

        // It is important that this event Logger is before the time logger.
        if (jsapResult.userSpecified("listLengthsOutput")) {
            eventLoggers.add(new FileClusterEventLogger(jsapResult.getString("listLengthsOutput")) {
                @Override
                public void onStart(long id, Iterable<String> query) {
                    List<Long> lengths = StreamSupport.stream(query.spliterator(), false).map(b -> {
                        try {
                            return indexMap.get(ALIAS).documents(b).frequency();
                        } catch (IOException e) {
                            return -1L;
                        }
                    }).collect(Collectors.toList());
                    log(id, Joiner.on(" ").join(lengths));
                }

                @Override
                public void onEnd(long id, Iterable<Object> results) {
                }

                @Override
                public String column() {
                    return "list-lengths";
                }

            });
        }

        if (jsapResult.userSpecified("queryLengthOutput")) {
            eventLoggers.add(new FileClusterEventLogger(jsapResult.getString("queryLengthOutput")) {
                @Override
                public void onStart(long id, Iterable<String> query) {
                    log(id, String.valueOf(StreamSupport.stream(query.spliterator(), false).count()));
                }

                @Override
                public void onEnd(long id, Iterable<Object> results) {

                }

                @Override
                public String column() {
                    return "id,length";
                }

            });
        }

        if (jsapResult.userSpecified("timeOutput")) {
            eventLoggers.add(new TimeClusterEventLogger(jsapResult.getString("timeOutput")));
        }

        if (jsapResult.userSpecified("resultOutput")) {
            eventLoggers.add(new ResultClusterEventLogger(jsapResult.getString("resultOutput")));
        }

        try(BufferedReader br = new BufferedReader(new FileReader(jsapResult.getString("input")))) {
            long id = 0;
            for (String query; (query = br.readLine()) != null; ) {

                TermProcessor termProcessor = termProcessors.get(ALIAS);
                List<String> processedTerms =
                        Lists.newArrayList(Splitter.on(' ').omitEmptyStrings().split(query))
                                .stream()
                                .map(t -> {
                                    MutableString m = new MutableString(t);
                                    termProcessor.processTerm(m);
                                    return m.toString();
                                })
                                .collect(Collectors.toList());

                for (EventLogger l : eventLoggers) l.onStart(id, processedTerms);

                ObjectArrayList<DocumentScoreInfo<Reference2ObjectMap<Index, SelectedInterval[]>>> r =
                        new ObjectArrayList<>();
                query = CharMatcher.is(',').replaceFrom(query, "");

                try {
                    int docs = engine.process(query, 0, k, r);
                } catch (Exception e) {
                    LOGGER.error(String.format("There was an error while processing query: %s", query), e);
                }

                for (EventLogger l : eventLoggers) l.onEnd(id,
                        r.stream().map(dsi -> dsi.document).collect(Collectors.toList()));
                id++;

            }
        }

    }

}
