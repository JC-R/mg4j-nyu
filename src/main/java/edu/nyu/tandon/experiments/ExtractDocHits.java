package edu.nyu.tandon.experiments;

import com.martiansoftware.jsap.*;
import edu.nyu.tandon.experiments.cluster.ExtractShardScores;
import edu.nyu.tandon.query.Query;
import edu.nyu.tandon.query.TerminatingQueryEngine;
import edu.nyu.tandon.utils.Utils;
import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.index.TermProcessor;
import it.unimi.di.big.mg4j.query.SelectedInterval;
import it.unimi.di.big.mg4j.query.nodes.QueryBuilderVisitorException;
import it.unimi.di.big.mg4j.query.parser.QueryParserException;
import it.unimi.di.big.mg4j.query.parser.SimpleParser;
import it.unimi.di.big.mg4j.search.DocumentIteratorBuilderVisitor;
import it.unimi.di.big.mg4j.search.score.DocumentScoreInfo;
import it.unimi.di.big.mg4j.search.score.Scorer;
import it.unimi.dsi.fastutil.Hash;
import it.unimi.dsi.fastutil.objects.*;
import org.apache.commons.configuration.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.util.List;

import static edu.nyu.tandon.query.Query.MAX_STEMMING;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class ExtractDocHits {

    public static final Logger LOGGER = LoggerFactory.getLogger(ExtractDocHits.class);


    public static long[] extract(TerminatingQueryEngine<Reference2ObjectMap<Index, SelectedInterval[]>> engine,
                               String inputFile, int k, int documentCount)
            throws IOException, QueryParserException, QueryBuilderVisitorException {
        try (BufferedReader queryReader = new BufferedReader(new FileReader(inputFile))) {
            long[] hits = new long[documentCount];
            String query;
            int queryIdx = 0;
            while ((query = queryReader.readLine()) != null) {
                List<String> terms = Utils.extractTerms(query, null);
                String processedQuery = String.join(" OR ", terms);
                System.err.println(String.format("Query %d: %s [%s]", queryIdx++, query, processedQuery));
                ObjectArrayList<DocumentScoreInfo<Reference2ObjectMap<Index, SelectedInterval[]>>> r = new ObjectArrayList<>();
                engine.process(processedQuery, 0, k, r);
                for (DocumentScoreInfo<Reference2ObjectMap<Index, SelectedInterval[]>> dsi : r) {
                    hits[(int)dsi.document]++;
                }
            }
            return hits;
        }
    }

    public static void printHits(long[] hits) {
        int document = 0;
        for (long hit : hits) {
            System.out.println(String.format("%d,%d", document, hit));
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
                        new FlaggedOption("scorer", JSAP.STRING_PARSER, "bm25", JSAP.NOT_REQUIRED, 'S', "scorer", "Scorer type (bm25 or ql)"),
                        new UnflaggedOption("basename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of the index."),
                        new UnflaggedOption("documents", JSAP.INTEGER_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "Number of documents in index.")
                });

        final JSAPResult jsapResult = jsap.parse(args);
        if (jsap.messagePrinted()) return;

        String basename = jsapResult.getString("basename");

        Scorer scorer = ExtractShardScores.resolveScorer(jsapResult.getString("scorer"));
        TerminatingQueryEngine engine = createQueryEngine(basename);
        engine.score(scorer);

        int k = jsapResult.userSpecified("topK") ? jsapResult.getInt("topK") : 1000;

        long[] hits = extract(engine, jsapResult.getString("input"), k, jsapResult.getInt("documents"));
        printHits(hits);
    }

}
