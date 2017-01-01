package edu.nyu.tandon.experiments.cluster;

import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.martiansoftware.jsap.*;
import edu.nyu.tandon.query.Query;
import edu.nyu.tandon.query.QueryEngine;
import edu.nyu.tandon.utils.Stats;
import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.index.IndexReader;
import it.unimi.di.big.mg4j.index.TermProcessor;
import it.unimi.di.big.mg4j.index.cluster.SelectiveQueryEngine;
import it.unimi.di.big.mg4j.query.SelectedInterval;
import it.unimi.di.big.mg4j.query.parser.SimpleParser;
import it.unimi.di.big.mg4j.search.DocumentIteratorBuilderVisitor;
import it.unimi.di.big.mg4j.search.score.DocumentScoreInfo;
import it.unimi.di.big.mg4j.search.score.Scorer;
import it.unimi.dsi.fastutil.Hash;
import it.unimi.dsi.fastutil.objects.*;
import it.unimi.dsi.lang.MutableString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.List;
import java.util.stream.Collectors;

import static edu.nyu.tandon.query.Query.MAX_STEMMING;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class ExtractCostFeatures {

    public static final Logger LOGGER = LoggerFactory.getLogger(ExtractCostFeatures.class);

    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws Exception {

        SimpleJSAP jsap = new SimpleJSAP(ExtractCostFeatures.class.getName(), ".",
                new Parameter[]{
                        new Switch("globalStatistics", 'g', "global-statistics", "Whether to use global statistics. Note that they need to be calculated: see ClusterGlobalStatistics."),
                        new FlaggedOption("input", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'i', "input", "The input file with queries delimited by new lines."),
                        new FlaggedOption("output", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'o', "output", "The output files basename."),
                        new FlaggedOption("topK", JSAP.INTEGER_PARSER, "1000", JSAP.NOT_REQUIRED, 'k', "top-k", "The engine will limit the result set to top k results. k=1,000 by default."),
                        new FlaggedOption("shardId", JSAP.INTEGER_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 's', "shard-id", "The shard ID."),
                        new FlaggedOption("scorer", JSAP.STRING_PARSER, "bm25", JSAP.NOT_REQUIRED, 'S', "scorer", "Scorer type (bm25 or ql)"),
                        new UnflaggedOption("basename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of the index.")
                });

        final JSAPResult jsapResult = jsap.parse(args);
        if (jsap.messagePrinted()) return;

        String basename = jsapResult.getString("basename");
        String[] basenameWeight = new String[]{basename};

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
        Scorer scorer = ExtractShardScores.resolveScorer(jsapResult.getString("scorer"));
        if (jsapResult.userSpecified("globalStatistics")) {
            LOGGER.info("Running queries with global statistics.");
            SelectiveQueryEngine.setGlobalStatistics(scorer, basename);
        }
        engine.score(scorer);

        int k = jsapResult.getInt("topK");
        String outputBasename = jsapResult.userSpecified("shardId") ?
                String.format("%s#%d", jsapResult.getString("output"), jsapResult.getInt("shardId")) :
                jsapResult.getString("output");

        FileWriter writer = new FileWriter(outputBasename);

        Index index = indexMap.get(indexMap.firstKey());
        IndexReader indexReader = index.getReader();

        long queryCount = 0;
        try(BufferedReader br = new BufferedReader(new FileReader(jsapResult.getString("input")))) {
            for (String query; (query = br.readLine()) != null; ) {

                ObjectArrayList<DocumentScoreInfo<Reference2ObjectMap<Index, SelectedInterval[]>>> r = new ObjectArrayList<>();
                query = CharMatcher.is(',').replaceFrom(query, "");

                TermProcessor termProcessor = termProcessors.get(indexMap.firstKey());
                List<String> processedTerms = Lists.newArrayList(Splitter.on(' ').omitEmptyStrings().split(query))
                            .stream()
                            .map(t -> {
                                MutableString m = new MutableString(t);
                                termProcessor.processTerm(m);
                                return m.toString();
                            })
                            .collect(Collectors.toList());

                /*
                 * Query features.
                 */
                double maxIdf = Double.MIN_VALUE;
                double minIdf = Double.MAX_VALUE;
                double sumAMeanScore = 0;
                double sumHMeanScore = 0;
                double maxGMeanScore = Double.MIN_VALUE;
                double minMaxScore = Double.MIN_VALUE;
                double minMaxNumPostings = Double.MIN_VALUE;
                double sumVarScore = 0;
                Stats hMeanScoreStats = new Stats();
                Stats maxNumPostingsStats = new Stats();
                Stats maxScoreStats = new Stats();

                for (String term : processedTerms) {
                    /*
                     * Term features.
                     */
                    double idf = Math.log((double) index.numberOfDocuments / (double) indexReader.documents(term).frequency());
                    double maxScore;
                    /* the number of postings with max score */
                    double maxNumPostings = 0;
                    Stats scoreStats = new Stats();
                    try {
                        engine.process(term, 0, k, r);
                        maxScore = r.get(0).score;
                        for (DocumentScoreInfo<Reference2ObjectMap<Index, SelectedInterval[]>> result : r) {
                            scoreStats.add(result.score);
                            if (result.score == maxScore) maxNumPostings++;
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(String.format("There was an error while processing query: %s", query), e);
                    }

                    /*
                     * Aggregate query features.
                     */
                    maxIdf = Math.max(maxIdf, idf);
                    minIdf = Math.min(minIdf, idf);
                    sumAMeanScore += scoreStats.aMean();
                    sumHMeanScore += scoreStats.hMean();
                    maxGMeanScore = Math.max(maxGMeanScore, scoreStats.gMean());
                    minMaxScore = Math.min(minMaxScore, maxScore);
                    minMaxNumPostings = Math.min(minMaxNumPostings, maxNumPostings);
                    sumVarScore += scoreStats.variance();

                    hMeanScoreStats.add(scoreStats.hMean());
                    maxNumPostingsStats.add(maxNumPostings);
                    maxScoreStats.add(maxScore);
                }

                long elapsed;
                try {
                    long start = System.currentTimeMillis();
                    engine.process(query, 0, k, r);
                    elapsed = System.currentTimeMillis() - start;
                    queryCount++;
                } catch (Exception e) {
                    LOGGER.error(String.format("There was an error while processing query: %s", query), e);
                    throw e;
                }

                writer.append(String.format("%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f",
                        (double) queryCount,
                        maxIdf,
                        minIdf,
                        sumAMeanScore,
                        sumHMeanScore,
                        maxGMeanScore,
                        minMaxScore,
                        minMaxNumPostings,
                        sumVarScore,
                        hMeanScoreStats.variance(),
                        maxNumPostingsStats.variance(),
                        maxScoreStats.variance(),
                        (double) elapsed
                ));

            }
        }

        writer.close();
        indexReader.close();

    }
}
