package edu.nyu.tandon.experiments;

import com.martiansoftware.jsap.*;
import edu.nyu.tandon.query.Query;
import edu.nyu.tandon.utils.Utils;
import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.index.TermProcessor;
import it.unimi.di.big.mg4j.query.QueryEngine;
import it.unimi.di.big.mg4j.query.SelectedInterval;
import it.unimi.di.big.mg4j.query.parser.SimpleParser;
import it.unimi.di.big.mg4j.search.DocumentIteratorBuilderVisitor;
import it.unimi.di.big.mg4j.search.score.DocumentScoreInfo;
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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.stream.IntStream;

import static edu.nyu.tandon.query.Query.MAX_STEMMING;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class ExtractDocHits {

    public static final Logger LOGGER = LoggerFactory.getLogger(ExtractDocHits.class);

    static class Hits {
        public long[] hits_10;
        public long[] hits_100;
        public long[] hits_1000;
        public int documentCount;
        public Hits(long[] hits_10, long[] hits_100, long[] hits_1000) {
            assert(hits_10.length == hits_100.length);
            assert(hits_100.length == hits_1000.length);
            this.documentCount = hits_10.length;
            this.hits_10 = hits_10;
            this.hits_100 = hits_100;
            this.hits_1000 = hits_1000;
        }
        public void add(Hits other) {
            assert(documentCount == other.documentCount);
            for (int idx = 0; idx < documentCount; ++idx) {
                hits_10[idx] += other.hits_10[idx];
                hits_100[idx] += other.hits_100[idx];
                hits_1000[idx] += other.hits_1000[idx];
            }
        }
    }

    static class Extract implements Callable {

        protected QueryEngine engine;
        protected String inputFile;
        protected int k;
        protected int documentCount;


        public Extract(QueryEngine engine, String inputFile, int k, int documentCount) {
            this.engine = engine;
            this.inputFile = inputFile;
            this.k = k;
            this.documentCount = documentCount;
        }

        @Override
        public Hits call() throws Exception {
            try (BufferedReader queryReader = new BufferedReader(new FileReader(inputFile))) {
                long[] hits_10 = new long[documentCount];
                long[] hits_100 = new long[documentCount];
                long[] hits_1000 = new long[documentCount];
                String query;
                int queryIdx = 0;
                while ((query = queryReader.readLine()) != null) {
                    System.out.println(query);
                    List<String> terms = Utils.extractTerms(query, null);
                    String processedQuery = String.join(" OR ", terms);
                    System.err.println(String.format("Query %d: %s [%s]", queryIdx++, query, processedQuery));
                    ObjectArrayList<DocumentScoreInfo<Reference2ObjectMap<Index, SelectedInterval[]>>> r = new ObjectArrayList<>();
                    try {
                        engine.process(processedQuery, 0, k, r);
                        for (int idx = 0; idx < Math.min(10, r.size()); idx++) {
                            int document = (int)r.get(idx).document;
                            hits_10[document]++;
                            hits_100[document]++;
                            hits_1000[document]++;
                        }
                        for (int idx = 10; idx < Math.min(100, r.size()); idx++) {
                            int document = (int)r.get(idx).document;
                            hits_100[document]++;
                            hits_1000[document]++;
                        }
                        for (int idx = 100; idx < r.size(); idx++) {
                            int document = (int)r.get(idx).document;
                            hits_1000[document]++;
                        }
                    } catch (Exception e) {
                        System.err.println(String.format("Failed to process query: %s", query));
                        e.printStackTrace();
                    }
                }
                return new Hits(hits_10, hits_100, hits_1000);
            }
        }
    }


    ///public static long[] extract(QueryEngine engine, String inputFile, int k, int documentCount)
    ///        throws IOException, QueryParserException, QueryBuilderVisitorException {
    ///    try (BufferedReader queryReader = new BufferedReader(new FileReader(inputFile))) {
    ///        long[] hits = new long[documentCount];
    ///        String query;
    ///        int queryIdx = 0;
    ///        while ((query = queryReader.readLine()) != null) {
    ///            List<String> terms = Utils.extractTerms(query, null);
    ///            String processedQuery = String.join(" OR ", terms);
    ///            System.err.println(String.format("Query %d: %s [%s]", queryIdx++, query, processedQuery));
    ///            ObjectArrayList<DocumentScoreInfo<Reference2ObjectMap<Index, SelectedInterval[]>>> r = new ObjectArrayList<>();
    ///            engine.process(processedQuery, 0, k, r);
    ///            for (DocumentScoreInfo<Reference2ObjectMap<Index, SelectedInterval[]>> dsi : r) {
    ///                hits[(int)dsi.document]++;
    ///            }
    ///        }
    ///        return hits;
    ///    }
    ///}

    public static void printHits(Hits hits) {
        System.out.println("docid,h10,h100,h1000");
        for (int document = 0; document < hits.documentCount; document++) {
            System.out.println(String.format("%d,%d,%d,%d",
                    document, hits.hits_10[document], hits.hits_100[document], hits.hits_1000[document]));
        }
    }

    public static List<QueryEngine>
    createQueryEngines(String basename, int numberOfCopies)
            throws IllegalAccessException, InvocationTargetException, NoSuchMethodException, IOException, InstantiationException, URISyntaxException, ConfigurationException, ClassNotFoundException {

        List<QueryEngine> engines = new ArrayList<>();
        for (int t = 0; t < numberOfCopies; t++) {
            String[] basenameWeight = new String[]{basename};
            final Object2ReferenceLinkedOpenHashMap<String, Index> indexMap = new Object2ReferenceLinkedOpenHashMap<>(Hash.DEFAULT_INITIAL_SIZE, .5f);
            final Reference2DoubleOpenHashMap<Index> index2Weight = new Reference2DoubleOpenHashMap<>();
            Query.loadIndicesFromSpec(basenameWeight, true, null, indexMap, index2Weight);
            final Object2ObjectOpenHashMap<String, TermProcessor> termProcessors = new Object2ObjectOpenHashMap<>(indexMap.size());
            for (String alias : indexMap.keySet()) termProcessors.put(alias, indexMap.get(alias).termProcessor);
            QueryEngine engine = new QueryEngine(
                    new SimpleParser(indexMap.keySet(), indexMap.firstKey(), termProcessors),
                    new DocumentIteratorBuilderVisitor(indexMap,
                            new Reference2ReferenceOpenHashMap<>(),
                            indexMap.get(indexMap.firstKey()),
                            MAX_STEMMING),
                    indexMap);
            engine.setWeights(index2Weight);
            engines.add(engine);
        }
        return engines;
    }

    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws Exception {

        SimpleJSAP jsap = new SimpleJSAP(Query.class.getName(), ".",
                new Parameter[]{
                        new FlaggedOption("input", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'i', "input", "The input file with queries delimited by new lines.")
                                .setAllowMultipleDeclarations(true),
                        new FlaggedOption("topK", JSAP.INTEGER_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'k', "top-k", "The engine will limit the result set to top k results."),
                        //new FlaggedOption("scorer", JSAP.STRING_PARSER, "bm25", JSAP.NOT_REQUIRED, 'S', "scorer", "Scorer type (bm25 or ql)"),
                        //new FlaggedOption("threads", JSAP.INTEGER_PARSER, "8", JSAP.NOT_REQUIRED, 't', "threads", "Number of threads to run in parallel."),
                        new UnflaggedOption("basename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of the index."),
                        new UnflaggedOption("documents", JSAP.INTEGER_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "Number of documents in index.")
                });

        final JSAPResult jsapResult = jsap.parse(args);
        if (jsap.messagePrinted()) return;

        String basename = jsapResult.getString("basename");
        String[] input = jsapResult.getStringArray("input");
        int threads = input.length;

        //Scorer scorer = ExtractShardScores.resolveScorer(jsapResult.getString("scorer"));
        List<QueryEngine> engine = createQueryEngines(basename, threads);

        int k = jsapResult.userSpecified("topK") ? jsapResult.getInt("topK") : 1000;
        int documents = jsapResult.getInt("documents");

        List<Future> results = new ArrayList<>();
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        IntStream.range(0, threads).forEach(i -> results.add(pool.submit(
                new Extract(engine.get(i), input[i], k, documents))));

        Hits hits = new Hits(new long[documents], new long[documents], new long[documents]);
        for (Future result : results) {
            try {
                Hits partial = (Hits)result.get();
                hits.add(partial);
            } catch (ExecutionException e) {
                e.printStackTrace();
                result.cancel(true);
            }
        }
        pool.shutdown();
        printHits(hits);
    }

}
