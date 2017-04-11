package edu.nyu.tandon.utils;

import com.google.common.base.Verify;
import com.google.common.io.Files;
import edu.nyu.tandon.query.Query;
import edu.nyu.tandon.search.score.BM25PrunedScorer;
import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.index.TermProcessor;
import it.unimi.di.big.mg4j.query.QueryEngine;
import it.unimi.di.big.mg4j.query.parser.SimpleParser;
import it.unimi.di.big.mg4j.search.DocumentIteratorBuilderVisitor;
import it.unimi.di.big.mg4j.search.score.BM25Scorer;
import it.unimi.dsi.fastutil.Hash;
import it.unimi.dsi.fastutil.longs.LongBigArrayBigList;
import it.unimi.dsi.fastutil.objects.*;
import org.apache.commons.configuration.ConfigurationException;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.util.Random;

import static edu.nyu.tandon.query.Query.MAX_STEMMING;
import static edu.nyu.tandon.tool.cluster.ClusterGlobalStatistics.loadGlobalFrequencies;
import static edu.nyu.tandon.tool.cluster.ClusterGlobalStatistics.loadGlobalStats;
import static java.lang.Integer.valueOf;

/**
 * Created by RodriguJ on 6/11/2015.
 */
public class Utils {
    /**
     * Returns a pseudo-random number between min and max, inclusive.
     * The difference between min and max can be at most
     * <code>Integer.MAX_VALUE - 1</code>.
     *
     * @param min Minimum value
     * @param max Maximum value.  Must be greater than min.
     * @return Integer between min and max, inclusive.
     * @see Random#nextInt(int)
     */
    public static int rand(int min, int max) {

        // NOTE: Usually this should be a field rather than a method
        // variable so that it is not re-seeded every call.
        Random rand = new Random();

        // nextInt is normally exclusive of the top value,
        // so add 1 to make it inclusive
        int randomNum = rand.nextInt((max - min) + 1) + min;

        return randomNum;
    }

    public static QueryEngine constructQueryEngine(String indexBasename) throws IllegalAccessException, InvocationTargetException, NoSuchMethodException, IOException, InstantiationException, URISyntaxException, ConfigurationException, ClassNotFoundException {
        return constructQueryEngine(indexBasename, false);
    }

    public static QueryEngine constructQueryEngine(String indexBasename, boolean loadGlobalStats) throws IllegalAccessException, InvocationTargetException, NoSuchMethodException, IOException, InstantiationException, URISyntaxException, ConfigurationException, ClassNotFoundException {

        String[] basenameWeight = new String[] { indexBasename };

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
        if (loadGlobalStats) {
            long[] globalStats = loadGlobalStats(indexBasename);
            scorer.setGlobalMetrics(globalStats[0], globalStats[1], loadGlobalFrequencies(indexBasename));
        }
        engine.score(scorer);

        return engine;
    }

    public static LongBigArrayBigList readMapping(String file, long numberOfDocuments) throws IOException {
        LongBigArrayBigList ints = new LongBigArrayBigList();
        ints.size(numberOfDocuments);
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            for (int i = 0; i < numberOfDocuments; i++) {
                ints.set(i, valueOf(reader.readLine()));
            }
        }
        return ints;
    }

    public static LongBigArrayBigList readMapping(File file) throws IOException {
        LongBigArrayBigList ints = new LongBigArrayBigList();
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            String val;
            long id = 0;
            while ((val = reader.readLine()) != null) {
                ints.set(id++, valueOf(val));
            }
        }
        return ints;
    }

    public static void unfolder(File file) throws IOException {
        Verify.verify(file.exists(), "the file doesn't exist");
        File[] parquetFiles = file.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File file, String s) {
                return s.endsWith("parquet");
            }
        });
        Verify.verify(parquetFiles.length == 1, "detected more than one parquet file in the folder");

        File parquet = parquetFiles[0];
        File dir = file.getParentFile();
        File temp = File.createTempFile(file.getAbsolutePath(), "", dir);
        Files.move(file, temp);
        Files.move(parquet, file);
        temp.delete();
    }
}
