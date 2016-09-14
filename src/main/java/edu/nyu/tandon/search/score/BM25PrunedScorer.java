package edu.nyu.tandon.search.score;

import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.index.IndexIterator;
import it.unimi.di.big.mg4j.search.AbstractIntersectionDocumentIterator;
import it.unimi.di.big.mg4j.search.DocumentIterator;
import it.unimi.di.big.mg4j.search.score.BM25Scorer;
import it.unimi.dsi.fastutil.ints.IntBigList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;

/**
 * Created by juan on 4/12/16.
 *
 *
 * BM25Scorer: modify original BM25 class to use global statistics for computing score, instead of local stats.
 * In particular, we want global term frequency (list length) instead of the local pruned list length, and global documents
 *
 */
public class BM25PrunedScorer extends BM25Scorer {

    public static final Logger LOGGER = LoggerFactory.getLogger(BM25PrunedScorer.class);

    public BM25PrunedScorer() {
        super();
        globalScoring = false;
    }
    public BM25PrunedScorer(final double a, final double b) {
        super(a,b);
        globalScoring = false;
    }
    public BM25PrunedScorer(final String k1, final String b) {
        super(k1, b);
        globalScoring = false;
    }
    public String toString() {
        return "BM25PrunedScorer()";
    }

    protected long g_numberOfDocuments;
    protected long g_numberOfOccurrences;
    protected boolean globalScoring;
    protected LongArrayList globalTermFrequencies;

    public void setGlobalMetrics(final long numdocs, final long numOcc, final LongArrayList list) {
        g_numberOfDocuments = numdocs;
        g_numberOfOccurrences = numOcc;
        globalTermFrequencies = list;
        globalScoring = (globalTermFrequencies != null);
    }

    public LongArrayList getGlobalTermFrequencies() { return this.globalTermFrequencies;}

//    @Override
    public void wraps(DocumentIterator d) throws IOException {

        long numDocs = this.g_numberOfDocuments;
        long numOccurrences = this.g_numberOfOccurrences;

        super.wrap(d);

        // need access to global term frequencies
        final Index[] index = termVisitor.indices();
        if (indexIterator != null && index.length == 1 && (documentIterator instanceof AbstractIntersectionDocumentIterator || indexIterator.length < MAX_FLAT_DISJUNCTS)) {
			/* This code is a flat, simplified duplication of what a CounterSetupVisitor would do. It is here just for efficiency. */

            if (flatIndexIterator.length != 0) {

                // index is pruned if global metrics are available
                if (!globalScoring) {
                    numDocs = flatIndexIterator[0].index().numberOfDocuments;
                    numOccurrences = flatIndexIterator[0].index().numberOfOccurrences;
                }

                // Some caching of frequently-used values
                k1TimesBDividedByAverageDocumentSize = k1 * b * numDocs / numOccurrences;
                if ((this.sizes = flatIndexIterator[0].index().sizes) == null)
                    throw new IllegalStateException("A BM25 scorer requires document sizes");

                // We do all logs here, and multiply by the weight
                for (int i = 0; i < numberOfPairs; i++) {
                    if (globalScoring) {
                        final long frequency = globalTermFrequencies.getLong((int)flatIndexIterator[i].termNumber());
                        k1Plus1TimesWeightedIdfPart[i] = (k1 + 1) * Math.max(EPSILON_SCORE,
                                Math.log((numDocs - frequency + 0.5) / (frequency + 0.5))) * index2Weight.getDouble(flatIndexIterator[i].index());
                    } else {
                        final long frequency = flatIndexIterator[i].frequency();
                        k1Plus1TimesWeightedIdfPart[i] = (k1 + 1) * Math.max(EPSILON_SCORE,
                                Math.log((numDocs - frequency + 0.5) / (frequency + 0.5))) * index2Weight.getDouble(flatIndexIterator[i].index());
                    }
                }
            }
        } else {
            // TODO: the OR greatest OR threat OR to OR the OR existence OR of OR the OR bald OR eagle OR came OR from OR the OR extensive OR use OR of OR ddt OR and OR other OR pesticides OR after OR world OR war OR ii
            throw new IllegalArgumentException("Multiple index queries not supported.");
        }

    }

    @Override
    public void wrap(DocumentIterator d) throws IOException {
        super.wrap(d);

        long numDocs = this.g_numberOfDocuments;
        long numOccurrences = this.g_numberOfOccurrences;

		/* Note that we use the index array provided by the weight function, *not* by the visitor or by the iterator.
         * If the function has an empty domain, this call is equivalent to prepare(). */
        termVisitor.prepare(index2Weight.keySet());

        d.accept(termVisitor);

        if (DEBUG) LOGGER.debug("Term Visitor found " + termVisitor.numberOfPairs() + " leaves");

        // Note that we use the index array provided by the visitor, *not* by the iterator.
        final Index[] index = termVisitor.indices();

        if (DEBUG) LOGGER.debug("Indices: " + Arrays.toString(index));

        flatIndexIterator = null;

		/* We use the flat evaluator only for single-index, term-only queries that are either quite small, and
         * then either conjunctive, or disjunctive with a reasonable number of terms. */

        if (indexIterator != null && index.length == 1) {
			/* This code is a flat, simplified duplication of what a CounterSetupVisitor would do. It is here just for efficiency. */
            numberOfPairs = 0;
			/* Find duplicate terms. We score unique pairs term/index with nonzero frequency, as the standard method would do. */
            final LongOpenHashSet alreadySeen = new LongOpenHashSet();

            for (int i = indexIterator.length; i-- != 0; )
                if (indexIterator[i].frequency() != 0 && alreadySeen.add(indexIterator[i].termNumber()))
                    numberOfPairs++;

            if (numberOfPairs == indexIterator.length) flatIndexIterator = indexIterator;
            else {
				/* We must compact the array, eliminating zero-frequency iterators. */
                flatIndexIterator = new IndexIterator[numberOfPairs];
                alreadySeen.clear();
                for (int i = 0, p = 0; i != indexIterator.length; i++)
                    if (indexIterator[i].frequency() != 0 && alreadySeen.add(indexIterator[i].termNumber()))
                        flatIndexIterator[p++] = indexIterator[i];
            }

            if (flatIndexIterator.length != 0) {

                // index is pruned if global metrics are available
                if (!globalScoring) {
                    numDocs = flatIndexIterator[0].index().numberOfDocuments;
                    numOccurrences = flatIndexIterator[0].index().numberOfOccurrences;
                }

                // Some caching of frequently-used values
                k1TimesBDividedByAverageDocumentSize = k1 * b * numDocs / numOccurrences;
                if ((this.sizes = flatIndexIterator[0].index().sizes) == null)
                    throw new IllegalStateException("A BM25 scorer requires document sizes");

                // We do all logs here, and multiply by the weight
                k1Plus1TimesWeightedIdfPart = new double[numberOfPairs];
                for (int i = 0; i < numberOfPairs; i++) {
                    long frequency;
                    if (globalScoring) frequency = globalTermFrequencies.getLong((int) flatIndexIterator[i].termNumber());
                    else frequency = flatIndexIterator[i].frequency();
                    k1Plus1TimesWeightedIdfPart[i] = (k1 + 1) * Math.max(EPSILON_SCORE,
                            Math.log((numDocs - frequency + 0.5) / (frequency + 0.5))) * index2Weight.getDouble(flatIndexIterator[i].index());
                }
            }
        } else {
            throw new IllegalArgumentException("Multiple index queries not supported.");
        }

    }

    @Override
    public synchronized BM25PrunedScorer copy() {
        final BM25PrunedScorer scorer = new BM25PrunedScorer(k1, b);
        scorer.setWeights(index2Weight);
        return scorer;
    }
}
