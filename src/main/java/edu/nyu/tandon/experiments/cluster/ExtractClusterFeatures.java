package edu.nyu.tandon.experiments.cluster;

import com.google.common.base.CharMatcher;
import com.martiansoftware.jsap.*;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.Iterator;

import static edu.nyu.tandon.query.Query.MAX_STEMMING;
import static edu.nyu.tandon.tool.cluster.ClusterGlobalStatistics.loadGlobalFrequencies;
import static edu.nyu.tandon.tool.cluster.ClusterGlobalStatistics.loadGlobalStats;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class ExtractClusterFeatures {

    public static final Logger LOGGER = LoggerFactory.getLogger(ExtractClusterFeatures.class);

    // TODO: Should be more general and support other kinds of aliases.
//    private static final String ALIAS = "text";

    public static void main(String[] args) throws Exception {

        SimpleJSAP jsap = new SimpleJSAP(Query.class.getName(), ".",
                new Parameter[]{
                        new Switch("globalStatistics", 'g', "global-statistics", "Whether to use global statistics. Note that they need to be calculated: see ClusterGlobalStatistics."),
                        new FlaggedOption("input", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'i', "input", "The input file with queries delimited by new lines."),
                        new FlaggedOption("output", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'o', "output", "The output files basename."),
                        new FlaggedOption("topK", JSAP.INTEGER_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'k', "top-k", "The engine will limit the result set to top k results. k=10 by default."),
                        new FlaggedOption("shardId", JSAP.INTEGER_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 's', "shard-id", "The shard ID."),
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

        QueryEngine engine = new QueryEngine<DocumentScoreInfo<Reference2ObjectMap<Index, SelectedInterval[]>>>(
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
        String outputBasename = jsapResult.userSpecified("shardId") ?
                String.format("%s#%d", jsapResult.getString("output"), jsapResult.getInt("shardId")) :
                jsapResult.getString("output");

        FileWriter resultWriter = new FileWriter(String.format("%s.results", outputBasename));
        FileWriter scoreWriter = new FileWriter(String.format("%s.scores", outputBasename));

        try(BufferedReader br = new BufferedReader(new FileReader(jsapResult.getString("input")))) {
            for (String query; (query = br.readLine()) != null; ) {

//                TermProcessor termProcessor = termProcessors.get(ALIAS);
//                List<String> processedTerms =
//                        Lists.newArrayList(Splitter.on(' ').omitEmptyStrings().split(query))
//                                .stream()
//                                .map(t -> {
//                                    MutableString m = new MutableString(t);
//                                    termProcessor.processTerm(m);
//                                    return m.toString();
//                                })
//                                .collect(Collectors.toList());

                ObjectArrayList<DocumentScoreInfo<Reference2ObjectMap<Index, SelectedInterval[]>>> r =
                        new ObjectArrayList<>();
                query = CharMatcher.is(',').replaceFrom(query, "");

                try {
                    engine.process(query, 0, k, r);
                } catch (Exception e) {
                    LOGGER.error(String.format("There was an error while processing query: %s", query), e);
                    throw e;
                }

                Iterator<DocumentScoreInfo<Reference2ObjectMap<Index, SelectedInterval[]>>> it = r.iterator();
                if (it.hasNext()) {
                    DocumentScoreInfo<Reference2ObjectMap<Index, SelectedInterval[]>> dsi = it.next();
                    resultWriter.append(String.valueOf(dsi.document));
                    scoreWriter.append(String.valueOf(dsi.score));
                    while (it.hasNext()) {
                        dsi = it.next();
                        resultWriter.append(' ').append(String.valueOf(dsi.document));
                        scoreWriter.append(' ').append(String.valueOf(dsi.score));
                    }
                }
                resultWriter.append('\n');
                scoreWriter.append('\n');

            }
        }

        resultWriter.close();
        scoreWriter.close();

    }

}
