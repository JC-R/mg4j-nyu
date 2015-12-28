package edu.nyu.tandon.query;

import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.query.SelectedInterval;
import it.unimi.di.big.mg4j.query.nodes.QueryBuilderVisitor;
import it.unimi.di.big.mg4j.query.parser.QueryParser;
import it.unimi.di.big.mg4j.search.DocumentIterator;
import it.unimi.di.big.mg4j.search.score.DocumentScoreInfo;
import it.unimi.di.big.mg4j.search.score.ScoredDocumentBoundedSizeQueue;
import it.unimi.dsi.fastutil.longs.LongSet;
import it.unimi.dsi.fastutil.objects.Object2ReferenceMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.Reference2ObjectMap;

import java.io.IOException;

import static it.unimi.di.big.mg4j.search.DocumentIterator.END_OF_LIST;

/**
 * Created by <michal.siedlaczek@nyu.edu> on 12/28/15.
 */
public class TerminatingQueryEngine<T> extends QueryEngine<T> {

    /**
     * Documents with IDs higher than the following won't be processed.
     */
    protected Long earlyTerminationThreshold;

    protected Object2ReferenceMap<String, Index> indexMap;

    public TerminatingQueryEngine(QueryParser queryParser, QueryBuilderVisitor<DocumentIterator> builderVisitor, Object2ReferenceMap<String, Index> indexMap) {
        super(queryParser, builderVisitor, indexMap);
        this.indexMap = indexMap;
        this.earlyTerminationThreshold = null;
    }

    public void setEarlyTerminationThreshold(Double p) {
        if (p == null) {
            earlyTerminationThreshold = null;
            return;
        }
        if (p < 0. || p > 1.) throw new RuntimeException("Early termination threshold has to be between 0 and 1");
        if (indexMap.size() != 1) throw new RuntimeException("Early termination works only for one index.");
        for (Index index : indexMap.values()) {
            /* TODO: Deal with sparse orderings. */
            earlyTerminationThreshold = Math.round(index.numberOfDocuments * p);
        }
    }

    protected boolean shouldTerminate(long documentId) {
        return earlyTerminationThreshold != null ? documentId > earlyTerminationThreshold : false;
    }

    @Override
    protected int getScoredResults(final DocumentIterator documentIterator, final int offset, final int length, final double lastMinScore, final ObjectArrayList<DocumentScoreInfo<T>> results, final LongSet alreadySeen) throws IOException {
        final ScoredDocumentBoundedSizeQueue<Reference2ObjectMap<Index, SelectedInterval[]>> top = new ScoredDocumentBoundedSizeQueue<Reference2ObjectMap<Index, SelectedInterval[]>>(offset + length);
        long document;
        int count = 0; // Number of not-already-seen documents

        scorer.wrap(documentIterator);
        // TODO: we should avoid enqueueing until we really know we shall use the values
        if (alreadySeen != null)
            while ((document = scorer.nextDocument()) != END_OF_LIST) {
                if (alreadySeen.add(document)) continue;
                if (shouldTerminate(document)) break;
                count++;
                top.enqueue(document, scorer.score());
            }
        else
            while ((document = scorer.nextDocument()) != END_OF_LIST) {
                if (shouldTerminate(document)) break;
                count++;
                top.enqueue(document, scorer.score());
            }

        final int n = Math.max(top.size() - offset, 0); // Number of actually useful documents, if any
        if (ASSERTS) assert n <= length : n;
        if (n > 0) {
            final int s = results.size();
            results.size(s + n);
            final Object[] elements = results.elements();
            // We scale all newly inserted item so that scores are always decreasing
            for (int i = n; i-- != 0; ) elements[i + s] = top.dequeue();
            // The division by the maximum score was missing in previous versions; can be removed to reproduce regressions.
            // TODO: this will change scores if offset leaves out an entire query
            final double adjustment = lastMinScore / (s != 0 ? ((DocumentScoreInfo<?>) elements[s]).score : 1.0);
            for (int i = n; i-- != 0; ) ((DocumentScoreInfo<?>) elements[i + s]).score *= adjustment;
        }

        return count;
    }

    @Override
    protected int getResults(final DocumentIterator documentIterator, final int offset, final int length, final ObjectArrayList<DocumentScoreInfo<T>> results, final LongSet alreadySeen) throws IOException {
        long document;
        int count = 0; // Number of not-already-seen documents

        // Unfortunately, to provide the exact count of results we have to scan the whole iterator.
        if (alreadySeen != null)
            while ((document = documentIterator.nextDocument()) != END_OF_LIST) {
                if (!alreadySeen.add(document)) continue;
                if (shouldTerminate(document)) break;
                if (count >= offset && count < offset + length) results.add(new DocumentScoreInfo<T>(document, -1));
                count++;
            }
        else if (length != 0)
            while ((document = documentIterator.nextDocument()) != END_OF_LIST) {
                if (shouldTerminate(document)) break;
                if (count < offset + length && count >= offset) results.add(new DocumentScoreInfo<T>(document, -1));
                count++;
            }
        else while ((document = documentIterator.nextDocument()) != END_OF_LIST) count++;

        return count;
    }

}
