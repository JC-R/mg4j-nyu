package edu.nyu.tandon.search.score;

import it.unimi.di.big.mg4j.index.IndexIterator;
import it.unimi.di.big.mg4j.search.score.BM25Scorer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class UnigramScorer extends BM25Scorer {
    static Logger LOGGER = LoggerFactory.getLogger(UnigramScorer.class);

    @Override
    public double score() throws IOException {

        final long document = documentIterator.document();

        if (flatIndexIterator == null) {
            throw new UnsupportedOperationException();
        } else {
            final double size = sizes.getInt(document);
            double score = 0;
            final IndexIterator[] actualIndexIterator = this.flatIndexIterator;

            for (int i = numberOfPairs; i-- != 0; ) {
                if (actualIndexIterator[i].document() == document) {
                    final int c = actualIndexIterator[i].count();
                    score += c / size;
                }
            }
            return score;
        }
    }

}
