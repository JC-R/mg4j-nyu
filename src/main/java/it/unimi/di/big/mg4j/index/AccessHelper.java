package it.unimi.di.big.mg4j.index;

import static it.unimi.di.big.mg4j.index.QuasiSuccinctIndexReader.*;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class AccessHelper {

    public static long getOccurrency(IndexIterator indexIterator) {
        if (indexIterator instanceof AbstractQuasiSuccinctIndexIterator) {
            AbstractQuasiSuccinctIndexIterator i = (AbstractQuasiSuccinctIndexIterator) indexIterator;
            return i.occurrency;
        }
        throw new UnsupportedOperationException(
                String.format("%s is not supported", indexIterator.getClass().getName()));
    }

}
