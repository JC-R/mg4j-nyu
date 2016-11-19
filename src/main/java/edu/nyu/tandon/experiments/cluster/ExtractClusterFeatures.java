package edu.nyu.tandon.experiments.cluster;

import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.martiansoftware.jsap.*;
import edu.nyu.tandon.query.Query;
import edu.nyu.tandon.query.QueryEngine;
import it.unimi.di.big.mg4j.index.Index;
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
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static edu.nyu.tandon.query.Query.MAX_STEMMING;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class ExtractClusterFeatures {

    public static final Logger LOGGER = LoggerFactory.getLogger(ExtractClusterFeatures.class);

    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws Exception {

        SimpleJSAP jsap = new SimpleJSAP(Query.class.getName(), ".",
                new Parameter[]{
                        new Switch("globalStatistics", 'g', "global-statistics", "Whether to use global statistics. Note that they need to be calculated: see ClusterGlobalStatistics."),
                        new FlaggedOption("input", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'i', "input", "The input file with queries delimited by new lines."),
                        new FlaggedOption("output", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'o', "output", "The output files basename."),
                        new FlaggedOption("topK", JSAP.INTEGER_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'k', "top-k", "The engine will limit the result set to top k results. k=10 by default."),
                        new FlaggedOption("shardId", JSAP.INTEGER_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 's', "shard-id", "The shard ID."),
                        new FlaggedOption("scorer", JSAP.STRING_PARSER, "bm25", JSAP.NOT_REQUIRED, 'S', "scorer", "Scorer type (bm25 or ql)"),
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
        Scorer scorer = ExtractShardScores.resolveScorer(jsapResult.getString("scorer"));
        if (jsapResult.userSpecified("globalStatistics")) {
            LOGGER.info("Running queries with global statistics.");
            SelectiveQueryEngine.setGlobalStatistics(scorer, basename);
        }
        engine.score(scorer);

        int k = jsapResult.userSpecified("topK") ? jsapResult.getInt("topK") : 10;
        String outputBasename = jsapResult.userSpecified("shardId") ?
                String.format("%s#%d", jsapResult.getString("output"), jsapResult.getInt("shardId")) :
                jsapResult.getString("output");
        String type = jsapResult.userSpecified("shardId") ? ".local" : ".global";

        FileWriter resultWriter = new FileWriter(String.format("%s.results%s", outputBasename, type));
        FileWriter scoreWriter = new FileWriter(String.format("%s.results.scores", outputBasename));
        FileWriter timeWriter = new FileWriter(String.format("%s.time", outputBasename));
        FileWriter avgTimeWriter = new FileWriter(String.format("%s.time.avg", outputBasename));
        FileWriter maxListLen1Writer = new FileWriter(String.format("%s.maxlist1", outputBasename));
        FileWriter maxListLen2Writer = new FileWriter(String.format("%s.maxlist2", outputBasename));
        FileWriter minListLen1Writer = new FileWriter(String.format("%s.minlist1", outputBasename));
        FileWriter minListLen2Writer = new FileWriter(String.format("%s.minlist2", outputBasename));
        FileWriter sumListLenWriter = new FileWriter(String.format("%s.sumlist", outputBasename));


        long totalTime = 0;
        long queryCount = 0;
        try(BufferedReader br = new BufferedReader(new FileReader(jsapResult.getString("input")))) {
            for (String query; (query = br.readLine()) != null; ) {

                ObjectArrayList<DocumentScoreInfo<Reference2ObjectMap<Index, SelectedInterval[]>>> r =
                        new ObjectArrayList<>();
                query = CharMatcher.is(',').replaceFrom(query, "");

                Index index = indexMap.get(indexMap.firstKey());
                TermProcessor termProcessor = termProcessors.get(indexMap.firstKey());
                List<String> processedTerms =
                        Lists.newArrayList(Splitter.on(' ').omitEmptyStrings().split(query))
                                .stream()
                                .map(t -> {
                                    MutableString m = new MutableString(t);
                                    termProcessor.processTerm(m);
                                    return m.toString();
                                })
                                .collect(Collectors.toList());
                List<Long> listLengths = processedTerms.stream().map(term -> {
                    try {
                        return index.documents(term).frequency();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }).collect(Collectors.toList());
                if (listLengths.isEmpty()) {
                    maxListLen1Writer.append("0\n");
                    maxListLen2Writer.append("0\n");
                    minListLen1Writer.append("0\n");
                    minListLen2Writer.append("0\n");
                }
                else {
                    maxListLen1Writer.append(String.valueOf(listLengths.get(0))).append('\n');
                    minListLen1Writer.append(String.valueOf(listLengths.get(listLengths.size() - 1))).append('\n');
                    if (listLengths.size() > 1) {
                        maxListLen2Writer.append(String.valueOf(listLengths.get(1))).append('\n');
                        minListLen2Writer.append(String.valueOf(listLengths.get(listLengths.size() - 2))).append('\n');
                    }
                    else {
                        maxListLen2Writer.append("0\n");
                        minListLen2Writer.append("0\n");
                    }
                }
                sumListLenWriter
                        .append(String.valueOf(listLengths.stream().mapToLong(Long::longValue).sum()))
                        .append('\n');

                try {
                    long start = System.currentTimeMillis();
                    engine.process(query, 0, k, r);
                    long elapsed = System.currentTimeMillis() - start;
                    totalTime += elapsed;
                    queryCount++;
                    timeWriter.append(String.valueOf(elapsed)).append('\n');
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

        avgTimeWriter.append(String.valueOf(totalTime / queryCount));
        avgTimeWriter.close();

        resultWriter.close();
        scoreWriter.close();
        timeWriter.close();
        maxListLen1Writer.close();
        maxListLen2Writer.close();
        minListLen1Writer.close();
        minListLen2Writer.close();
        sumListLenWriter.close();

    }

}
