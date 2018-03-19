package edu.nyu.tandon.experiments.l2rr;

import com.google.common.primitives.Ints;
import com.martiansoftware.jsap.*;
import edu.nyu.tandon.experiments.cluster.ExtractShardScores;
import edu.nyu.tandon.query.Query;
import edu.nyu.tandon.query.TerminatingQueryEngine;
import edu.nyu.tandon.utils.Utils;
import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.index.TermProcessor;
import it.unimi.di.big.mg4j.index.cluster.DocumentalPartitioningStrategy;
import it.unimi.di.big.mg4j.index.cluster.SelectiveQueryEngine;
import it.unimi.di.big.mg4j.query.SelectedInterval;
import it.unimi.di.big.mg4j.query.nodes.QueryBuilderVisitorException;
import it.unimi.di.big.mg4j.query.parser.QueryParserException;
import it.unimi.di.big.mg4j.query.parser.SimpleParser;
import it.unimi.di.big.mg4j.search.DocumentIteratorBuilderVisitor;
import it.unimi.di.big.mg4j.search.score.DocumentScoreInfo;
import it.unimi.di.big.mg4j.search.score.Scorer;
import it.unimi.dsi.fastutil.Hash;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.objects.*;
import org.apache.commons.configuration.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;

import static edu.nyu.tandon.query.Query.MAX_STEMMING;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class ExtractChampionListFeatures {

    public static final Logger LOGGER = LoggerFactory.getLogger(ExtractChampionListFeatures.class);

    public static int[][] hitsForTerms(TerminatingQueryEngine<Reference2ObjectMap<Index, SelectedInterval[]>> engine,
                                       List<String> terms,
                                       List<Integer> ks,
                                       DocumentalPartitioningStrategy strategy) throws QueryParserException, QueryBuilderVisitorException, IOException {
        int maxK = ks.get(ks.size() - 1);
        int[][] hits = new int[ks.size()][strategy.numberOfLocalIndices()];
        for (String term : terms) {
            ObjectArrayList<DocumentScoreInfo<Reference2ObjectMap<Index, SelectedInterval[]>>> r = new ObjectArrayList<>();
            engine.process(term, 0, maxK, r);
            int kidx = 0;
            int currentIdx = ks.get(0);
            for (DocumentScoreInfo<Reference2ObjectMap<Index, SelectedInterval[]>> dsi : r) {
                hits[kidx][strategy.localIndex(dsi.document)]++;
                currentIdx++;
                if (currentIdx == ks.get(kidx)) {
                    kidx++;
                    System.arraycopy(hits[kidx - 1], 0, hits[kidx], 0, strategy.numberOfLocalIndices());
                }
            }
        }
        return hits;
    }

    public static void printHits(int[][] hits, int clusters, int query) {
        for (int shard = 0; shard < clusters; shard++) {
            StringBuilder line = new StringBuilder();
            line.append(query);
            for (int[] karr : hits) line.append(',').append(karr[shard]);
            System.out.println(line);
        }
    }

    public static void extract(TerminatingQueryEngine<Reference2ObjectMap<Index, SelectedInterval[]>> engine,
                               DocumentalPartitioningStrategy strategy, String inputFile, List<Integer> ks)
            throws IOException, QueryParserException, QueryBuilderVisitorException {
        assert (ks != null && ks.size() > 0);
        Collections.sort(ks);
        try (BufferedReader queryReader = new BufferedReader(new FileReader(inputFile))) {
            String query;
            int queryIdx = 0;
            while ((query = queryReader.readLine()) != null) {
                List<String> terms = Utils.extractTerms(query, null);
                int[][] hits = hitsForTerms(engine, terms, ks, strategy);
                printHits(hits, strategy.numberOfLocalIndices(), queryIdx++);
            }
        }
    }

    public static TerminatingQueryEngine<Reference2ObjectMap<Index, SelectedInterval[]>>
    createQueryEngine(String basename) throws IllegalAccessException, InvocationTargetException, NoSuchMethodException, IOException, InstantiationException, URISyntaxException, ConfigurationException, ClassNotFoundException {
        String[] basenameWeight = new String[]{basename};
        final Object2ReferenceLinkedOpenHashMap<String, Index> indexMap = new Object2ReferenceLinkedOpenHashMap<>(Hash.DEFAULT_INITIAL_SIZE, .5f);
        final Reference2DoubleOpenHashMap<Index> index2Weight = new Reference2DoubleOpenHashMap<>();
        Query.loadIndicesFromSpec(basenameWeight, true, null, indexMap, index2Weight);

        final Object2ObjectOpenHashMap<String, TermProcessor> termProcessors = new Object2ObjectOpenHashMap<>(indexMap.size());
        for (String alias : indexMap.keySet()) termProcessors.put(alias, indexMap.get(alias).termProcessor);
        final SimpleParser simpleParser = new SimpleParser(indexMap.keySet(), indexMap.firstKey(), termProcessors);
        final Reference2ReferenceMap<Index, Object> index2Parser = new Reference2ReferenceOpenHashMap<>();

        TerminatingQueryEngine<Reference2ObjectMap<Index, SelectedInterval[]>> engine =
                new TerminatingQueryEngine<>(
                        simpleParser,
                        new DocumentIteratorBuilderVisitor(indexMap, index2Parser, indexMap.get(indexMap.firstKey()), MAX_STEMMING),
                        indexMap);
        engine.setWeights(index2Weight);
        return engine;
    }

    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws Exception {

        SimpleJSAP jsap = new SimpleJSAP(Query.class.getName(), ".",
                new Parameter[]{
                        new FlaggedOption("input", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'i', "input", "The input file with queries delimited by new lines."),
                        new FlaggedOption("topK", JSAP.INTEGER_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'k', "top-k", "The engine will limit the result set to top k results."),
                        //new FlaggedOption("buckets", JSAP.INTEGER_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'b', "buckets", "Partition results into this many buckets."),
                        new FlaggedOption("scorer", JSAP.STRING_PARSER, "bm25", JSAP.NOT_REQUIRED, 'S', "scorer", "Scorer type (bm25 or ql)"),
                        new UnflaggedOption("basename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of the index."),
                        new UnflaggedOption("strategy", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The clustering strategy.")
                });

        final JSAPResult jsapResult = jsap.parse(args);
        if (jsap.messagePrinted()) return;

        //int buckets = jsapResult.userSpecified("buckets") ? jsapResult.getInt("buckets") : 0;

        String basename = jsapResult.getString("basename");

        Scorer scorer = ExtractShardScores.resolveScorer(jsapResult.getString("scorer"));
        SelectiveQueryEngine.setGlobalStatistics(scorer, basename);
        TerminatingQueryEngine engine = createQueryEngine(basename);
        engine.score(scorer);

        int[] k = jsapResult.userSpecified("topK") ? jsapResult.getIntArray("topK") : new int[]{10, 100};

        DocumentalPartitioningStrategy strategy = (DocumentalPartitioningStrategy)
                BinIO.loadObject(jsapResult.getString("strategy"));

        extract(engine, strategy, jsapResult.getString("input"), Ints.asList(k));
    }

}
