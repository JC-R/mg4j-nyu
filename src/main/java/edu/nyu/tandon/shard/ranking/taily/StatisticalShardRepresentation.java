package edu.nyu.tandon.shard.ranking.taily;

import com.martiansoftware.jsap.*;
import edu.nyu.tandon.query.Query;
import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.index.IndexAccessHelper;
import it.unimi.di.big.mg4j.index.IndexIterator;
import it.unimi.di.big.mg4j.index.IndexReader;
import it.unimi.dsi.fastutil.longs.LongBigArrayBigList;
import org.apache.commons.configuration.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Iterator;

import static edu.nyu.tandon.tool.cluster.ClusterGlobalStatistics.loadGlobalOccurrencies;
import static edu.nyu.tandon.tool.cluster.ClusterGlobalStatistics.loadGlobalStats;
import static it.unimi.di.big.mg4j.search.DocumentIterator.END_OF_LIST;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class StatisticalShardRepresentation {

    public static final Logger LOGGER = LoggerFactory.getLogger(StatisticalShardRepresentation.class);

    public static class Term {
        public double expectedValue;
        public double variance;
        public double minValue;

        public Term(double expectedValue, double variance, double minValue) {
            this.expectedValue = expectedValue;
            this.variance = variance;
            this.minValue = minValue;
        }
    }

    protected interface TermIterator extends Iterator<Term> {
        Term skip(long n) throws IOException;
        void close() throws IOException;
    }

    private static final String EXPECTED_V_SUFFIX = ".exp";
    private static final String VARIANCE_SUFFIX = ".var";
    private static final String MIN_SCORE_SUFFIX = ".minscore";

    /**
     * Dirichlet smoothing parameter.
     */
    protected double mu;

    private String basename;

//    private boolean useGlobalStatistics = false;
//    private LongBigArrayBigList globalOccurrencies;
//    private long globalCollectionSize;

    public StatisticalShardRepresentation(String basename) {
        this.basename = basename;
    }

//    public StatisticalShardRepresentation withGlobalMetrics(long collectionSize, final LongBigArrayBigList occurrencies) {
//        this.useGlobalStatistics = true;
//        this.globalCollectionSize = collectionSize;
//        this.globalOccurrencies = occurrencies;
//        return this;
//    }

    protected TermIterator calc() throws IllegalAccessException, URISyntaxException, IOException, InstantiationException, NoSuchMethodException, ConfigurationException, InvocationTargetException, ClassNotFoundException {
        return calc(2500);
    }

    protected TermIterator calc(double mu) throws IllegalAccessException, InvocationTargetException, NoSuchMethodException, IOException, InstantiationException, URISyntaxException, ConfigurationException, ClassNotFoundException {
        this.mu = mu;
        return new TermIterator() {

            private IndexReader indexReader = Index.getInstance(basename, true, true).getReader();
            private IndexIterator iterator = indexReader.nextIterator();

            @Override
            public boolean hasNext() {
                return iterator != null;
            }

            @Override
            public Term next() {
                if (iterator == null) return null;
                try {
                    Term next = termStats(iterator);
                    bufferNext();
                    return next;
                } catch (IOException e) {
                    throw new RuntimeException(String.format("Error while calculating stats of term: %s",
                            iterator.term()), e);
                }
            }

            @Override
            public Term skip(long n) throws IOException {
                for (long i = 0; i < n; i++) bufferNext();
                return next();
            }

            @Override
            public void close() throws IOException {
                indexReader.close();
            }

            private void bufferNext() throws IOException {
                if (iterator != null) {
                    iterator = indexReader.nextIterator();
                    if (iterator == null) indexReader.close();
                }
            }
        };
    }

    protected long occurrency(IndexIterator indexIterator) throws IOException {
        return IndexAccessHelper.getOccurrency(indexIterator);
//        return useGlobalStatistics
//                ? globalOccurrencies.getLong(indexIterator.termNumber())
//                : IndexAccessHelper.getOccurrency(indexIterator);
    }

    protected long collectionSize(IndexIterator indexIterator) throws IOException {
        return indexIterator.index().numberOfPostings;
//        return useGlobalStatistics
//                ? globalCollectionSize
//                : indexIterator.index().numberOfPostings;
    }

    protected long documentSize(IndexIterator indexIterator) {
        if (indexIterator.index().sizes == null) throw new IllegalStateException("index has no document sizes");
        return indexIterator.index().sizes.getInt(indexIterator.document());
    }

    protected Term termStats(IndexIterator indexIterator) throws IOException {
        double sum = 0;
        double sumOfSquares = 0;
        double frequency = indexIterator.frequency();
        double minValue = 0;
        while (indexIterator.nextDocument() != END_OF_LIST) {
            double prior = (double) occurrency(indexIterator) / collectionSize(indexIterator);
            double numerator = (double) indexIterator.count() + mu * prior;
            double denominator = (double) documentSize(indexIterator) + mu;
            double score = Math.log(numerator) - Math.log(denominator);
            minValue = Math.min(minValue, score);
            sum += score;
            sumOfSquares += score * score;
        }
        double expectedValue = sum / frequency;
        double expectedSquaredValue = sumOfSquares / frequency;
        double variance = expectedSquaredValue - expectedValue * expectedValue;
        return new Term(expectedValue, variance, minValue);
    }

    public void write(Iterator<Term> terms) throws IOException {
        try (DataOutputStream expectedStream = new DataOutputStream(new FileOutputStream(basename + EXPECTED_V_SUFFIX));
             DataOutputStream varianceStream = new DataOutputStream(new FileOutputStream(basename + VARIANCE_SUFFIX));
             DataOutputStream minScoreStream = new DataOutputStream(new FileOutputStream(basename + MIN_SCORE_SUFFIX))) {
            while (terms.hasNext()) {
                Term term = terms.next();
                expectedStream.writeDouble(term.expectedValue);
                varianceStream.writeDouble(term.variance);
                minScoreStream.writeDouble(term.minValue);
            }
        }
    }

    public Term queryScore(long[] termIds) throws IOException, IllegalAccessException, URISyntaxException, InstantiationException, ConfigurationException, NoSuchMethodException, InvocationTargetException, ClassNotFoundException {
        Arrays.sort(termIds);
        double expectedValue = 0;
        double variance = 0;
        for (long termId : termIds) {
            Term term = termIterator().skip(termId);
            expectedValue += term.expectedValue + term.minValue;
            variance += term.variance;
        }
        return new Term(expectedValue, variance, 0);
    }

    public TermIterator termIterator() throws IOException, IllegalAccessException, InvocationTargetException, InstantiationException, NoSuchMethodException, URISyntaxException, ConfigurationException, ClassNotFoundException {
        return new TermIterator() {

            private DataInputStream expectedStream = new DataInputStream(new FileInputStream(basename + EXPECTED_V_SUFFIX));
            private DataInputStream varianceStream = new DataInputStream(new FileInputStream(basename + VARIANCE_SUFFIX));
            private DataInputStream minScoreStream = new DataInputStream(new FileInputStream(basename + MIN_SCORE_SUFFIX));

            private long remainingTerms = Index.getInstance(basename).numberOfTerms;

            @Override
            public boolean hasNext() {
                return remainingTerms > 0;
            }

            @Override
            public Term next() {
                try {
                    remainingTerms--;
                    double expectedValue = expectedStream.readDouble();
                    double variance = varianceStream.readDouble();
                    double minScore = minScoreStream.readDouble();
                    return new Term(expectedValue, variance, minScore);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            private void skipDataInputStream(DataInputStream in, long n) throws IOException {
                long skipped = 0;
                while (skipped < n) {
                    int intSkip = n <= Integer.MAX_VALUE
                            ? (int) n
                            : Integer.MAX_VALUE;
                    skipped += in.skipBytes(intSkip);
                }
            }

            @Override
            public Term skip(long n) throws IOException {
                skipDataInputStream(expectedStream, n);
                skipDataInputStream(varianceStream, n);
                skipDataInputStream(minScoreStream, n);
                remainingTerms -= n;
                return next();
            }

            @Override
            public void close() throws IOException {
                expectedStream.close();
                varianceStream.close();
                minScoreStream.close();
            }
        };
    }

    public static void main(String[] args) throws Exception {

        SimpleJSAP jsap = new SimpleJSAP(Query.class.getName(), ".",
                new Parameter[]{
//                        new Switch("globalStatistics", 'g', "global-statistics", "Whether to use global statistics. Note that they need to be calculated: see ClusterGlobalStatistics."),
                        new UnflaggedOption("basename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of the index.")
                });

        final JSAPResult jsapResult = jsap.parse(args);
        if (jsap.messagePrinted()) return;

        String basename = jsapResult.getString("basename");
        StatisticalShardRepresentation ssr = new StatisticalShardRepresentation(basename);
//        if (jsapResult.userSpecified("globalStatistics")) {
//            LOGGER.info("Running queries with global statistics.");
//            long[] globalStats = loadGlobalStats(basename);
//            LongBigArrayBigList globalOccurrencies = loadGlobalOccurrencies(basename);
//            ssr.withGlobalMetrics(globalStats[1], globalOccurrencies);
//        }
        ssr.write(ssr.calc());

    }

}
