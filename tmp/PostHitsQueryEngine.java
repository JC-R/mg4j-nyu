package edu.nyu.tandon.query;

import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.index.IndexIterator;

import it.unimi.di.big.mg4j.query.SelectedInterval;
import it.unimi.di.big.mg4j.query.nodes.Query;
import it.unimi.di.big.mg4j.query.nodes.QueryBuilderVisitor;
import it.unimi.di.big.mg4j.query.nodes.QueryBuilderVisitorException;

import it.unimi.di.big.mg4j.query.parser.QueryParser;
import it.unimi.di.big.mg4j.query.parser.QueryParserException;
import it.unimi.di.big.mg4j.search.AbstractCompositeDocumentIterator;
import it.unimi.di.big.mg4j.search.DocumentIterator;
import it.unimi.di.big.mg4j.search.OrDocumentIterator;
import it.unimi.di.big.mg4j.search.score.DocumentScoreInfo;
import it.unimi.di.big.mg4j.search.score.ScoredDocumentBoundedSizeQueue;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import it.unimi.dsi.fastutil.objects.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import static it.unimi.di.big.mg4j.search.DocumentIterator.END_OF_LIST;

/**
 * Created by RodriguJ on 6/11/2015.
 */

public class PostHitsQueryEngine<T> extends QueryEngine<T> {

    static long [] bits = {
            0x0000000000000000L,
            0x0000000000000001L,
            0x0000000000000002L,
            0x0000000000000004L,
            0x0000000000000008L,
            0x0000000000000010L,
            0x0000000000000020L,
            0x0000000000000040L,
            0x0000000000000080L,
            0x0000000000000100L,
            0x0000000000000200L,
            0x0000000000000400L,
            0x0000000000000800L,
            0x0000000000001000L,
            0x0000000000002000L,
            0x0000000000004000L,
            0x0000000000008000L,
            0x0000000000010000L,
            0x0000000000020000L,
            0x0000000000040000L,
            0x0000000000080000L,
            0x0000000000100000L,
            0x0000000000200000L,
            0x0000000000400000L,
            0x0000000000800000L,
            0x0000000001000000L,
            0x0000000002000000L,
            0x0000000003000000L,
            0x0000000004000000L,
            0x0000000008000000L,
            0x0000000010000000L,
            0x0000000020000000L,
            0x0000000040000000L,
            0x0000000080000000L,
            0x0000000100000000L,
            0x0000000200000000L,
            0x0000000400000000L,
            0x0000000800000000L,
            0x0000001000000000L,
            0x0000002000000000L,
            0x0000004000000000L,
            0x0000008000000000L,
            0x0000010000000000L,
            0x0000020000000000L,
            0x0000040000000000L,
            0x0000080000000000L,
            0x0000100000000000L,
            0x0000200000000000L,
            0x0000400000000000L,
            0x0000800000000000L,
            0x0001000000000000L,
            0x0002000000000000L,
            0x0004000000000000L,
            0x0008000000000000L,
            0x0010000000000000L,
            0x0020000000000000L,
            0x0040000000000000L,
            0x0080000000000000L,
            0x0100000000000000L,
            0x0200000000000000L,
            0x0400000000000000L,
            0x0800000000000000L,
            0x1000000000000000L,
            0x2000000000000000L,
            0x4000000000000000L
    };

    private long bitEncode(int v) {
        if (v>=bits.length-1) return 0L;
        return bits[v];
    }

    private final Logger LOGGER = LoggerFactory.getLogger(PostHitsQueryEngine.class);

    public ArrayList<Long> currTerms = new ArrayList<Long>();

    @Override
    public int getScoredResults( final DocumentIterator documentIterator, final int offset, final int length, final double lastMinScore,
                                 final ObjectArrayList<DocumentScoreInfo<T>> results, final LongSet alreadySeen ) throws IOException {

        final ScoredDocumentBoundedSizeQueue<T> top = new ScoredDocumentBoundedSizeQueue<T>( offset + length );
        long document;
        int count = 0; // Number of not-already-seen documents

        scorer.wrap( documentIterator );

        // TODO: we should avoid enqueueing until we really know we shall use the values
        // need to score entire list
        if ( alreadySeen != null )
            while ( ( document = scorer.nextDocument() ) != END_OF_LIST ) {
                if ( alreadySeen.add( document ) ) continue;
                count++;
                long terms = 0L;

                if (currTerms.size()<=1) {
                    terms = 1L;
                }
                else {
                    for (int j=0; j<currTerms.size(); j++) {
                        if (((OrDocumentIterator)documentIterator).queue.refArray[j] == document)
                            terms |= bitEncode(j+1);
                    }
                }
                top.enqueue( document, scorer.score(), terms);
            }
        else
            while ( ( document = scorer.nextDocument() ) != END_OF_LIST ) {
                count++;
                long terms = 0L;
                if (currTerms.size()<=1) {
                    terms = 1L;
                }
                else {
                    for (int j=0; j<currTerms.size(); j++) {
                        if (((OrDocumentIterator)documentIterator).queue.refArray[j] == document)
                            terms |= bitEncode(j+1);
                    }
                }
                top.enqueue( document, scorer.score(), terms );
            }

        final int n = Math.max( top.size() - offset, 0 ); // Number of actually useful documents, if any
        if ( ASSERTS ) assert n <= length : n;
        if ( n > 0 ) {
            final int s = results.size();
            results.size( s + n );
            final Object[] elements = results.elements();
            // We scale all newly inserted item so that scores are always decreasing
            for ( int i = n; i-- != 0; ) elements[ i + s ] = top.dequeue();
            // The division by the maximum score was missing in previous versions; can be removed to reproduce regressions.
            // TODO: this will change scores if offset leaves out an entire query
            final double adjustment = lastMinScore / ( s != 0 ? ((DocumentScoreInfo<?>)elements[ s ]).score : 1.0 );
            for ( int i = n; i-- != 0; ) {
                ((DocumentScoreInfo<?>)elements[ i + s ]).score *= adjustment;
            }
        }
        return count;
    }

//    @Override
//    public int  process( final Query query[], final int offset, final int length, final ObjectArrayList<DocumentScoreInfo<Long>> results ) throws QueryBuilderVisitorException, IOException {
//
//        LOGGER.debug( "Processing Query array \"" + Arrays.toString(query) + "\", offset=" + offset + ", length="+ length );
//        results.clear();
//        double lastMinScore = 1;
//        int total = 0, count, currOffset = offset, currLength = length;
//        final LongSet alreadySeen = query.length > 1 ? new LongOpenHashSet() : null;
//
//        currTerms.clear();
//
//        for( int i = 0; i < query.length; i++ ) {
//
//            final int initialResultSize = results.size();
//            DocumentIterator documentIterator = query[ i ].accept( builderVisitor.prepare() );
//
//            // preserve term numbers
//            currTerms = new ArrayList<Long>();
//
//            int numberOfTerms = 0;
//
//            if (documentIterator instanceof AbstractCompositeDocumentIterator) {
//                numberOfTerms = ((AbstractCompositeDocumentIterator) documentIterator).n;
//                for (int j = 0; j < numberOfTerms; j++)
//                    currTerms.add(((IndexIterator) (((DocumentIterator[]) ((AbstractCompositeDocumentIterator) documentIterator).documentIterator)[j])).termNumber());
//            }
//            else if (documentIterator instanceof IndexIterator) {
//                numberOfTerms = 1;
//                currTerms.add(((IndexIterator) documentIterator).termNumber());
//            }
//            else {
//                throw new RuntimeException();
//            }
//            count = scorer != null?
//                    getScoredResults( documentIterator, currOffset, currLength, lastMinScore, results, alreadySeen ) :
//                    getResults( documentIterator, currOffset, currLength, results, alreadySeen );
//
//             // TODO: use the scored documents to revisit the term lists and count posting hits
//
//            documentIterator.dispose();
//
//            if ( results.size() > 0 ) lastMinScore = results.get( results.size() - 1 ).score;
//
//            total += count;
//            currOffset -= count;
//
//            if ( currOffset < 0 ) {
//                currLength += currOffset;
//                currOffset = 0;
//            }
//        }
//        return total;
//    }

    /** Processes one or more pre-parsed queries and deposits in a given array a segment of the
     * results corresponding to the queries, using the current settings of this query engine.
     *
     * <p>Results are accumulated with an &ldquo;and-then&rdquo; semantics: results
     * are added from each query in order, provided they did not appear before.
     *
     * @param query an array of queries.
     * @param offset the first result to be added to <code>results</code>.
     * @param length the number of results to be added to <code>results</code>
     * @param results an array list that will hold all results.
     * @return the number of documents scanned while filling <code>results</code>.
     */
    @SuppressWarnings("unchecked")
    @Override
    public int process(final Query query[], final int offset, final int length, final ObjectArrayList<DocumentScoreInfo<T>> results ) throws QueryBuilderVisitorException, IOException {

        LOGGER.debug( "Processing Query array \"" + Arrays.toString( query ) + "\", offset=" + offset + ", length="+ length );
        results.clear();
        double lastMinScore = 1;
        int total = 0, count, currOffset = offset, currLength = length;
        final LongSet alreadySeen = query.length > 1 ? new LongOpenHashSet() : null;

        for( int i = 0; i < query.length; i++ ) {
            final int initialResultSize = results.size();

            DocumentIterator documentIterator = query[ i ].accept( builderVisitor.prepare() );

            count = scorer != null?
                    getScoredResults( documentIterator, currOffset, currLength, lastMinScore, results, alreadySeen ) :
                    getResults( documentIterator, currOffset, currLength, results, alreadySeen );

            documentIterator.dispose();
            if ( results.size() > 0 ) lastMinScore = results.get( results.size() - 1 ).score;

            total += count;
            currOffset -= count;

            if ( currOffset < 0 ) {
                currLength += currOffset;
                currOffset = 0;
            }

            // Check whether we have intervals, we want intervals *and* we added some results.
            boolean someHavePositions = false;
            for( Index index: documentIterator.indices() ) someHavePositions |= index.hasPositions;

            if ( someHavePositions && intervalSelector != null && results.size() != initialResultSize ) {
                // We must now enrich the returned result with intervals
                DocumentScoreInfo<Reference2ObjectMap<Index,SelectedInterval[]>> sorted[] =
                        results.subList( initialResultSize, results.size() ).toArray( new DocumentScoreInfo[ results.size() - initialResultSize ] );
                ObjectArrays.quickSort( sorted, DocumentScoreInfo.DOCUMENT_COMPARATOR );

                documentIterator = query[ i ].accept( builderVisitor.prepare() );

                for( DocumentScoreInfo<Reference2ObjectMap<Index,SelectedInterval[]>> dsi: sorted ) {
                    documentIterator.skipTo( dsi.document );
                    dsi.info = intervalSelector.select( documentIterator, new Reference2ObjectArrayMap<Index,SelectedInterval[]>( numIndices ) );
                }

                documentIterator.dispose();
            }

            if ( ASSERTS ) assert length >= results.size();
            if ( length == results.size() ) break;
        }
        return total;
    }

}
