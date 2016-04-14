package edu.nyu.tandon.search.score;

import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.index.IndexIterator;
import it.unimi.di.big.mg4j.search.AbstractIntersectionDocumentIterator;
import it.unimi.di.big.mg4j.search.DocumentIterator;
import it.unimi.di.big.mg4j.search.score.BM25Scorer;
import it.unimi.di.big.mg4j.search.score.DelegatingScorer;
import it.unimi.dsi.fastutil.ints.IntBigList;
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
 * In particular, we want global term frequency (list length) instead of the local pruned list length
 *
 */
public class BM25PrunedScorer extends BM25Scorer implements DelegatingScorer, {

    public static final Logger LOGGER = LoggerFactory.getLogger(BM25PrunedScorer.class);

    public BM25PrunedScorer() {super();}
    public BM25PrunedScorer(final double a, final double b) {super(a,b);}
    public String toString() {
        return "BM25PrunedScorer()";
    }

    @Override
    public void wrap(DocumentIterator d) throws IOException {
        super.wrap(d);

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

        if (indexIterator != null && index.length == 1 && (documentIterator instanceof AbstractIntersectionDocumentIterator || indexIterator.length < MAX_FLAT_DISJUNCTS)) {
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
                // Some caching of frequently-used values
                k1TimesBDividedByAverageDocumentSize = k1 * b * flatIndexIterator[0].index().numberOfDocuments / flatIndexIterator[0].index().numberOfOccurrences;
                if ((this.sizes = flatIndexIterator[0].index().sizes) == null)
                    throw new IllegalStateException("A BM25 scorer requires document sizes");

                // We do all logs here, and multiply by the weight
                k1Plus1TimesWeightedIdfPart = new double[numberOfPairs];
                for (int i = 0; i < numberOfPairs; i++) {
                    final long frequency = flatIndexIterator[i].frequency();
                    k1Plus1TimesWeightedIdfPart[i] = (k1 + 1) * Math.max(EPSILON_SCORE,
                            Math.log((flatIndexIterator[i].index().numberOfDocuments - frequency + 0.5) / (frequency + 0.5))) * index2Weight.getDouble(flatIndexIterator[i].index());
                }
            }
        } else {
            // Some caching of frequently-used values
            final double[] k1TimesBDividedByAverageDocumentSize = new double[index.length];
            for (int i = index.length; i-- != 0; )
                k1TimesBDividedByAverageDocumentSize[i] = k1 * b * index[i].numberOfDocuments / index[i].numberOfOccurrences;

            if (DEBUG) LOGGER.debug("Average document sizes: " + Arrays.toString(k1TimesBDividedByAverageDocumentSize));
            final IntBigList[] sizes = new IntBigList[index.length];
            for (int i = index.length; i-- != 0; )
                if ((sizes[i] = index[i].sizes) == null)
                    throw new IllegalStateException("A BM25 scorer requires document sizes");

            setupVisitor.prepare();
            d.accept(setupVisitor);
            numberOfPairs = termVisitor.numberOfPairs();
            final long[] frequency = setupVisitor.frequency;
            final int[] indexNumber = setupVisitor.indexNumber;

            // We do all logs here, and multiply by the weight
            k1Plus1TimesWeightedIdfPart = new double[frequency.length];
            for (int i = k1Plus1TimesWeightedIdfPart.length; i-- != 0; )
                k1Plus1TimesWeightedIdfPart[i] = (k1 + 1) * Math.max(EPSILON_SCORE,
                        Math.log((index[indexNumber[i]].numberOfDocuments - frequency[i] + 0.5) / (frequency[i] + 0.5))) * index2Weight.getDouble(index[indexNumber[i]]);

            visitor = new Visitor(k1Times1MinusB, k1Plus1TimesWeightedIdfPart, k1TimesBDividedByAverageDocumentSize, termVisitor.indices().length, indexNumber, sizes);
        }

    }
}
