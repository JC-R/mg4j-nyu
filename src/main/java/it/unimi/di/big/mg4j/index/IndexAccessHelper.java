package it.unimi.di.big.mg4j.index;

import it.unimi.di.big.mg4j.index.cluster.ClusterAccessHelper;
import it.unimi.di.big.mg4j.index.cluster.DocumentalConcatenatedClusterIndexIterator;
import it.unimi.di.big.mg4j.index.cluster.DocumentalMergedClusterIndexIterator;

import java.io.IOException;

import static it.unimi.di.big.mg4j.index.QuasiSuccinctIndexReader.*;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class IndexAccessHelper {

    public static long getOccurrency(IndexIterator indexIterator) throws IOException {
        if (indexIterator instanceof AbstractQuasiSuccinctIndexIterator) {
            AbstractQuasiSuccinctIndexIterator i = (AbstractQuasiSuccinctIndexIterator) indexIterator;
            return i.occurrency;
        }
        else if (indexIterator instanceof DocumentalMergedClusterIndexIterator) {
            DocumentalMergedClusterIndexIterator i = (DocumentalMergedClusterIndexIterator) indexIterator;
            long occurrency = 0;
            for (Index index : ClusterAccessHelper.getLocalIndices(ClusterAccessHelper.getIndex(i))) {
                occurrency += getOccurrency(index.documents(indexIterator.term()));
            }
            return occurrency;
        }
        else if (indexIterator instanceof DocumentalConcatenatedClusterIndexIterator) {
            DocumentalConcatenatedClusterIndexIterator i = (DocumentalConcatenatedClusterIndexIterator) indexIterator;
            long occurrency = 0;
            for (Index index : ClusterAccessHelper.getLocalIndices(ClusterAccessHelper.getIndex(i))) {
                occurrency += getOccurrency(index.documents(indexIterator.term()));
            }
            return occurrency;
        }
        else if (indexIterator instanceof Index.EmptyIndexIterator) {
            return 0;
        }
        throw new UnsupportedOperationException(
                String.format("%s is not supported", indexIterator.getClass().getName()));
    }

}
