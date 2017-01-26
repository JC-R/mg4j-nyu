package edu.nyu.tandon.shard.ranking.taily;

import com.martiansoftware.jsap.*;
import edu.nyu.tandon.query.Query;
import edu.nyu.tandon.utils.LineIterator;
import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.index.IndexAccessHelper;
import it.unimi.di.big.mg4j.index.IndexIterator;
import it.unimi.di.big.mg4j.index.IndexReader;
import it.unimi.di.big.mg4j.index.cluster.ClusterAccessHelper;
import it.unimi.di.big.mg4j.index.cluster.DocumentalCluster;
import org.apache.commons.configuration.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static it.unimi.di.big.mg4j.search.DocumentIterator.END_OF_LIST;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class StatisticalShardRepresentation {

    public class TermStats {
        public double expectedValue;
        public double variance;
        public double minValue;

        public TermStats(double expectedValue, double variance, double minValue) {
            this.expectedValue = expectedValue;
            this.variance = variance;
            this.minValue = minValue;
        }

        @Override
        public String toString() {
            return String.format("[exp=%f, var=%f, min=%f]", expectedValue, variance, minValue);
        }
    }

    public interface TermIterator extends Iterator<TermStats> {
        TermStats next(long n) throws IOException;
        void close() throws IOException;
    }

    public class ClusterTermIterator implements TermIterator {

        protected IndexReader[] shardReaders;
        protected LineIterator[] termIterators;
        protected IndexIterator[] shardIterators;
        protected String[] currentTerms;
        protected int shardCount;
        protected String nextTerm;
        protected long collectionSize;

        public ClusterTermIterator(DocumentalCluster index, String basename) throws IOException {
            collectionSize = index.numberOfOccurrences;
            Index[] shards = ClusterAccessHelper.getLocalIndices(index);
            shardReaders = new IndexReader[shards.length];
            shardIterators = new IndexIterator[shards.length];
            termIterators = new LineIterator[shards.length];
            currentTerms = new String[shards.length];
            shardCount = shardReaders.length;
            for (int i = 0; i < shards.length; i++) {
                shardReaders[i] = shards[i].getReader();
                termIterators[i] = LineIterator.fromFile(String.format("%s-%d.terms", basename, i));
            }
            ge("");
        }

        protected void bufferNext(int i) throws IOException {
            shardIterators[i] = shardReaders[i].nextIterator();
            currentTerms[i] = termIterators[i].next();
        }

        protected String ge(int i, String term) throws IOException {
            assert term != null;
            if (shardIterators[i] == null) bufferNext(i);
            while (shardIterators[i] != null && currentTerms[i].compareTo(term) < 0) {
                bufferNext(i);
            }
            return currentTerms[i];
        }

        /**
         *
         * @param term the term we want to process next
         * @return a list of shards that have <code>term</code> at the pointer after execution
         * @throws IOException
         */
        protected List<Integer> ge(String term) throws IOException {
            List<Integer> shards = new ArrayList<>(shardCount);
            nextTerm = null;
            for (int i = 0; i < shardCount; i++) {
                String shardTerm = ge(i, term);
                if (shardTerm != null && shardTerm.equals(term)) {
                    shards.add(i);
                }
                else {
                    nextTerm = min(nextTerm, shardTerm);
                }
            }
            return shards;
        }

        protected String min(String s1, String s2) {
            if (s1 == null) return s2;
            if (s2 == null) return s1;
            if (s1.compareTo(s2) <= 0) return s1;
            return s2;
        }

        @Override
        public TermStats next(long n) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() throws IOException {
            for (IndexReader shardReader : shardReaders) shardReader.close();
            for (LineIterator termIterator : termIterators) termIterator.close();
        }

        @Override
        public boolean hasNext() {
            return nextTerm != null;
        }

        @Override
        public TermStats next() {
            try {
                String currentTerm = nextTerm;
                List<Integer> shards = ge(currentTerm);
                IndexIterator[] ii = new IndexIterator[shards.size()];
                for (int i = 0; i < shards.size(); i++) ii[i] = shardIterators[shards.get(i)];
                TermStats t = termStats(ii, collectionSize);
                for (Integer i : shards) {
                    String shardTerm = ge(i, currentTerm + Character.MIN_VALUE);
                    nextTerm = min(nextTerm, shardTerm);
                }
                return t;
            } catch (IOException e) {
                throw new RuntimeException(String.format("Error while calculating stats of term: %s",
                        nextTerm), e);
            }
        }

    }

    public class SingleIndexTermIterator implements TermIterator {

        private IndexReader indexReader;
        private IndexIterator iterator;

        public SingleIndexTermIterator(Index index) throws IOException, IllegalAccessException, URISyntaxException, InstantiationException, ConfigurationException, NoSuchMethodException, InvocationTargetException, ClassNotFoundException {
            indexReader = index.getReader();
            iterator =  indexReader.nextIterator();
        }

        @Override
        public boolean hasNext() {
            return iterator != null;
        }

        @Override
        public TermStats next() {
            if (iterator == null) return null;
            try {
                TermStats next = termStats(iterator, iterator.index().numberOfOccurrences);
                bufferNext();
                return next;
            } catch (IOException e) {
                throw new RuntimeException(String.format("Error while calculating stats of term: %s",
                        iterator.term()), e);
            }
        }

        @Override
        public TermStats next(long n) throws IOException {
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

    }

    public static final Logger LOGGER = LoggerFactory.getLogger(StatisticalShardRepresentation.class);

    private static final String EXPECTED_V_SUFFIX = ".exp";
    private static final String VARIANCE_SUFFIX = ".var";
    private static final String MIN_SCORE_SUFFIX = ".minscore";

    /**
     * Dirichlet smoothing parameter.
     */
    protected double mu;

    private String basename;
    private Index index;

    public StatisticalShardRepresentation(String basename) throws IllegalAccessException, InvocationTargetException, NoSuchMethodException, IOException, InstantiationException, URISyntaxException, ConfigurationException, ClassNotFoundException {
        this.basename = basename;
    }

    public StatisticalShardRepresentation(String basename, Index index) {
        this.basename = basename;
        this.index = index;
    }

    protected Index getIndex() throws IllegalAccessException, InvocationTargetException, NoSuchMethodException, IOException, InstantiationException, URISyntaxException, ConfigurationException, ClassNotFoundException {
        if (index == null) index = Index.getInstance(basename, true, true);
        return index;
    }

    protected TermIterator calc() throws IllegalAccessException, URISyntaxException, IOException, InstantiationException, NoSuchMethodException, ConfigurationException, InvocationTargetException, ClassNotFoundException {
        return calc(2500);
    }

    protected TermIterator calc(double mu) throws IllegalAccessException, InvocationTargetException, NoSuchMethodException, IOException, InstantiationException, URISyntaxException, ConfigurationException, ClassNotFoundException {
        this.mu = mu;
        if (getIndex() instanceof DocumentalCluster) {
            return new ClusterTermIterator((DocumentalCluster) getIndex(), basename);
        }
        else {
            return new SingleIndexTermIterator(index);
        }
    }

    protected long occurrency(IndexIterator indexIterator) throws IOException {
        return IndexAccessHelper.getOccurrency(indexIterator);
    }

    protected long documentSize(IndexIterator indexIterator) {
        if (indexIterator.index().sizes == null) throw new IllegalStateException("index has no document sizes");
        return indexIterator.index().sizes.getInt(indexIterator.document());
    }

    protected TermStats termStats(IndexIterator indexIterator, long collectionSize) throws IOException {
        return termStats(new IndexIterator[] {indexIterator}, collectionSize);
    }

    protected TermStats termStats(IndexIterator[] indexIterators, long collectionSize) throws IOException {
        double sum = 0;
        double sumOfSquares = 0;
        double frequency = 0;
        double minValue = 0;
        double occurrency = 0.0;
        for (IndexIterator indexIterator : indexIterators) {
            occurrency += (double) occurrency(indexIterator);
        }
        for (IndexIterator indexIterator : indexIterators) {
            frequency += indexIterator.frequency();
            while (indexIterator.nextDocument() != END_OF_LIST) {
                double prior = occurrency / collectionSize;
                double numerator = (double) indexIterator.count() + mu * prior;
                double denominator = (double) documentSize(indexIterator) + mu;
                double score = Math.log(numerator) - Math.log(denominator);
                minValue = Math.min(minValue, score);
                sum += score;
                sumOfSquares += score * score;
            }
        }
        double expectedValue = sum / frequency;
        double expectedSquaredValue = sumOfSquares / frequency;
        double variance = expectedSquaredValue - expectedValue * expectedValue;
        return new TermStats(expectedValue, variance, minValue);
    }

    public class ProgressTimerTask extends TimerTask {

        long start = System.currentTimeMillis();
        public long processed = 0;

        @Override
        public void run() {
            LOGGER.info(String.format("%s\tProcessed terms: %d",
                    elapsedFormatted(),
                    processed));
        }

        public String elapsedFormatted() {
            long elapsedInMillis = System.currentTimeMillis() - start;
            final long hr = TimeUnit.MILLISECONDS.toHours(elapsedInMillis);
            final long min = TimeUnit.MILLISECONDS.toMinutes(elapsedInMillis - TimeUnit.HOURS.toMillis(hr));
            final long sec = TimeUnit.MILLISECONDS.toSeconds(elapsedInMillis - TimeUnit.HOURS.toMillis(hr) - TimeUnit.MINUTES.toMillis(min));
            return String.format("%02d:%02d:%02d", hr, min, sec);
        }
    }

    public void write(Iterator<TermStats> terms) throws IOException {
        try (DataOutputStream expectedStream = new DataOutputStream(new FileOutputStream(basename + EXPECTED_V_SUFFIX));
             DataOutputStream varianceStream = new DataOutputStream(new FileOutputStream(basename + VARIANCE_SUFFIX));
             DataOutputStream minScoreStream = new DataOutputStream(new FileOutputStream(basename + MIN_SCORE_SUFFIX))) {
            ProgressTimerTask timerTask = new ProgressTimerTask();
            Timer timer = new Timer(basename);
            timer.scheduleAtFixedRate(timerTask, 0, 5000);
            while (terms.hasNext()) {
                TermStats term = terms.next();
                expectedStream.writeDouble(term.expectedValue);
                varianceStream.writeDouble(term.variance);
                minScoreStream.writeDouble(term.minValue);
                timerTask.processed++;
            }
            LOGGER.info(String.format("Finished processing %s in %s (%d terms processed)",
                    basename, timerTask.elapsedFormatted(), timerTask.processed));
            timer.cancel();
        }
    }

    public TermStats queryStats(long[] termIds) throws IOException, IllegalAccessException, URISyntaxException, InstantiationException, ConfigurationException, NoSuchMethodException, InvocationTargetException, ClassNotFoundException {
        Arrays.sort(termIds);
        double expectedValue = 0;
        double variance = 0;
        double minValue = 0;
        long prev = 0;
        TermIterator it = termIterator();
        for (long termId : termIds) {
            TermStats term = it.next(termId - prev);
            expectedValue += term.expectedValue;
            variance += term.variance;
            minValue += term.minValue;
            prev = termId;
        }
        it.close();
        return new TermStats(expectedValue, variance, minValue);
    }

    public TermIterator termIterator() throws IOException, IllegalAccessException, InvocationTargetException, InstantiationException, NoSuchMethodException, URISyntaxException, ConfigurationException, ClassNotFoundException {
        return new TermIterator() {

            private DataInputStream expectedStream = new DataInputStream(new FileInputStream(basename + EXPECTED_V_SUFFIX));
            private DataInputStream varianceStream = new DataInputStream(new FileInputStream(basename + VARIANCE_SUFFIX));
            private DataInputStream minScoreStream = new DataInputStream(new FileInputStream(basename + MIN_SCORE_SUFFIX));

            private long remainingTerms = getIndex().numberOfTerms;

            @Override
            public boolean hasNext() {
                return remainingTerms > 0;
            }

            @Override
            public TermStats next() {
                try {
                    remainingTerms--;
                    double expectedValue = expectedStream.readDouble();
                    double variance = varianceStream.readDouble();
                    double minScore = minScoreStream.readDouble();
                    return new TermStats(expectedValue, variance, minScore);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            private void skipDataInputStream(DataInputStream in, long n) throws IOException {
                while (n > 0) {
                    int intSkip = (n <= Integer.MAX_VALUE
                            ? (int) n
                            : Integer.MAX_VALUE) * 8;
                    n -= in.skipBytes(intSkip);
                }
            }

            @Override
            public TermStats next(long n) throws IOException {
                assert n >= 0;
                if (n > 0) {
                    skipDataInputStream(expectedStream, n - 1);
                    skipDataInputStream(varianceStream, n - 1);
                    skipDataInputStream(minScoreStream, n - 1);
                    remainingTerms -= n - 1;
                }
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
                        new UnflaggedOption("basename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of the index.")
                });

        final JSAPResult jsapResult = jsap.parse(args);
        if (jsap.messagePrinted()) return;

        String basename = jsapResult.getString("basename");
        StatisticalShardRepresentation ssr = new StatisticalShardRepresentation(basename);
        ssr.write(ssr.calc());

    }

}
