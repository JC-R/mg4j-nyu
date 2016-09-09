package edu.nyu.tandon.search.score;

import it.unimi.di.big.mg4j.index.AccessHelper;
import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.index.IndexIterator;
import it.unimi.di.big.mg4j.search.DocumentIterator;
import it.unimi.di.big.mg4j.search.score.BM25Scorer;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
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

    public QueryLikelihoodScorer() {
    }

    public QueryLikelihoodScorer(double mu) {
        this.mu = mu;
    }

    @Override
    public double score() throws IOException {

        final long document = documentIterator.document();

        if (flatIndexIterator == null) {
            throw new UnsupportedOperationException();
        } else {
            final double documentSize = sizes.getInt(document);

            double score = numberOfPairs > 0 ? 1 : 0;
            final IndexIterator[] actualIndexIterator = this.flatIndexIterator;

            for (int i = numberOfPairs; i-- != 0; ) {
                if (actualIndexIterator[i].document() == document) {

                    final double documentCount = actualIndexIterator[i].count();
                    final double collectionCount = AccessHelper.getOccurrency(actualIndexIterator[i]);

                    final double numerator = documentCount + mu * collectionCount / collectionSize;
                    final double denominator = documentSize + mu;
                    score *= numerator / denominator;
                }
                else {
                    score = 0;
                }
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
