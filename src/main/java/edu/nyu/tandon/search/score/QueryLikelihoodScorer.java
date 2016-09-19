package edu.nyu.tandon.search.score;

import it.unimi.di.big.mg4j.index.IndexAccessHelper;
import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.index.IndexIterator;
import it.unimi.di.big.mg4j.search.DocumentIterator;
import it.unimi.di.big.mg4j.search.score.BM25Scorer;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongBigArrayBigList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Scores documents according to query likelihood with Dirichlet prior smoothing.
 *
 * @author michal.siedlaczek@nyu.edu
 */
public class QueryLikelihoodScorer extends BM25Scorer {
    static Logger LOGGER = LoggerFactory.getLogger(QueryLikelihoodScorer.class);

    /**
     * Dirichlet smoothing parameter.
     */
    protected double mu = 2500;

    /**
     * The number of all occurrences in the index.
     */
    protected double collectionSize;

    /**
     * Use global statistics.
     */
    protected boolean useGlobalStatistics = false;

    /**
     * Global occurrencies.
     */
    protected LongBigArrayBigList occurrencies;

    public QueryLikelihoodScorer() {
    }

    public QueryLikelihoodScorer(double mu) {
        this.mu = mu;
    }

    public QueryLikelihoodScorer withGlobalMetrics(double collectionSize, final LongBigArrayBigList occurrencies) {
        this.setGlobalMetrics(collectionSize, occurrencies);
        return this;
    }

    public void setGlobalMetrics(double collectionSize, final LongBigArrayBigList occurrencies) {
        this.useGlobalStatistics = true;
        this.collectionSize = collectionSize;
        this.occurrencies = occurrencies;
    }

    @Override
    public double score() throws IOException {

        final long document = documentIterator.document();

        if (flatIndexIterator == null) {
            throw new UnsupportedOperationException();
        } else {

            final double documentSize = sizes.getInt(document);

            double score = 0;
            final IndexIterator[] actualIndexIterator = this.flatIndexIterator;

            for (int i = numberOfPairs; i-- != 0; ) {

                double documentCount = 0;
                if (actualIndexIterator[i].document() == document) {
                    documentCount = actualIndexIterator[i].count();
                }

                double collectionCount;
                if (useGlobalStatistics) {
                    collectionCount = occurrencies.getLong(actualIndexIterator[i].termNumber());
                }
                else {
                    collectionCount = IndexAccessHelper.getOccurrency(actualIndexIterator[i]);
                }

                final double numerator = documentCount + mu * collectionCount / collectionSize;
                final double denominator = documentSize + mu;
                score += Math.log(numerator) - Math.log(denominator);
            }
            return score;
        }
    }

    public void wrap(DocumentIterator d) throws IOException {
        super.wrap(d);
        if (d.indices().size() != 1) {
            throw new UnsupportedOperationException("Multiple indices are not supported");
        }
        Index index = d.indices().iterator().next();
        collectionSize = index.numberOfOccurrences;
    }

    @Override
    public synchronized QueryLikelihoodScorer copy() {
        final QueryLikelihoodScorer scorer = new QueryLikelihoodScorer();
        scorer.setWeights(index2Weight);
        return scorer;
    }

}
