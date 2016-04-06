package edu.nyu.tandon.query;

import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.query.nodes.QueryBuilderVisitor;
import it.unimi.di.big.mg4j.query.parser.QueryParser;
import it.unimi.di.big.mg4j.search.DocumentIterator;
import it.unimi.di.big.mg4j.search.IntervalIterator;
import it.unimi.di.big.mg4j.search.score.DocumentScoreInfo;
import it.unimi.di.big.mg4j.search.visitor.DocumentIteratorVisitor;
import it.unimi.dsi.fastutil.longs.LongSet;
import it.unimi.dsi.fastutil.objects.Object2ReferenceMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.Reference2ReferenceMap;
import it.unimi.dsi.fastutil.objects.ReferenceSet;

import java.io.IOException;

/**
 * Query egine that simulates tiers by defining lower and upper bounds
 * on document IDs that will be processed.
 * @param <T>
 */
public class TerminatingQueryEngine<T> extends QueryEngine<T> {

    /**
     * Documents having IDs lower than the following will be skipped at the beginning.
     */
    protected Long documentLowerBound;
    /**
     * Documents having IDs higher than the following won't be processed.
     */
    protected Long earlyTerminationThreshold;
    protected Object2ReferenceMap<String, Index> indexMap;

    public TerminatingQueryEngine(QueryParser queryParser, QueryBuilderVisitor<DocumentIterator> builderVisitor, Object2ReferenceMap<String, Index> indexMap) {
        super(queryParser, builderVisitor, indexMap);
        this.indexMap = indexMap;
        earlyTerminationThreshold = null;
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

    public void setDocumentLowerBound(Double p) {
        if (p == null) {
            documentLowerBound = null;
            return;
        }
        if (p < 0. || p > 1.) throw new RuntimeException("Document lower bound has to be between 0 and 1");
        if (indexMap.size() != 1) throw new RuntimeException("Document lower bound works only for one index.");
        for (Index index : indexMap.values()) {
            /* TODO: Deal with sparse orderings. */
            documentLowerBound = Math.round(index.numberOfDocuments * p);
        }
    }

    @Override
    protected int getScoredResults(final DocumentIterator documentIterator, final int offset, final int length, final double lastMinScore, final ObjectArrayList<DocumentScoreInfo<T>> results, final LongSet alreadySeen) throws IOException {
        return super.getScoredResults(new TerminatingDocumentIteratorWrapper(documentIterator),
                offset, length, lastMinScore, results, alreadySeen);
    }

    @Override
    protected int getResults(final DocumentIterator documentIterator, final int offset, final int length, final ObjectArrayList<DocumentScoreInfo<T>> results, final LongSet alreadySeen) throws IOException {
        return super.getResults(
                new TerminatingDocumentIteratorWrapper(documentIterator), offset, length, results, alreadySeen);
    }

    private class TerminatingDocumentIteratorWrapper implements DocumentIterator {

        DocumentIterator documentIterator;

        public TerminatingDocumentIteratorWrapper(DocumentIterator documentIterator) {
            this.documentIterator = documentIterator;
        }

        @Override
        public IntervalIterator intervalIterator() throws IOException {
            return documentIterator.intervalIterator();
        }

        @Override
        public IntervalIterator intervalIterator(Index index) throws IOException {
            return documentIterator.intervalIterator(index);
        }

        @Override
        public Reference2ReferenceMap<Index, IntervalIterator> intervalIterators() throws IOException {
            return documentIterator.intervalIterators();
        }

        @Override
        public ReferenceSet<Index> indices() {
            return documentIterator.indices();
        }

        protected long nextDocumentWithoutEarlyTermination() throws IOException {
            if (documentIterator.document() > -1 || documentLowerBound == null) return documentIterator.nextDocument();
            if (earlyTerminationThreshold != null && documentLowerBound >= earlyTerminationThreshold) {
                throw new RuntimeException(String.format(
                        "Document lower bound (%d) has to be lower than early termination threshold (%d).",
                        documentLowerBound,
                        earlyTerminationThreshold));
            }
            return documentIterator.skipTo(documentLowerBound);
        }

        @Override
        public long nextDocument() throws IOException {
            long documentId = nextDocumentWithoutEarlyTermination();
            if (earlyTerminationThreshold != null && documentId >= earlyTerminationThreshold) return END_OF_LIST;
            return documentId;
        }

        @Override
        public boolean mayHaveNext() {
            return documentIterator.mayHaveNext();
        }

        @Override
        public long document() {
            return documentIterator.document();
        }

        @Override
        public long skipTo(long l) throws IOException {
            return documentIterator.skipTo(l);
        }

        @Override
        public <Q> Q accept(DocumentIteratorVisitor<Q> documentIteratorVisitor) throws IOException {
            return documentIterator.accept(documentIteratorVisitor);
        }

        @Override
        public <Q> Q acceptOnTruePaths(DocumentIteratorVisitor<Q> documentIteratorVisitor) throws IOException {
            return documentIterator.acceptOnTruePaths(documentIteratorVisitor);
        }

        @Override
        public double weight() {
            return documentIterator.weight();
        }

        @Override
        public DocumentIterator weight(double v) {
            return documentIterator.weight(v);
        }

        @Override
        public void dispose() throws IOException {
            documentIterator.dispose();
        }
    }

}
