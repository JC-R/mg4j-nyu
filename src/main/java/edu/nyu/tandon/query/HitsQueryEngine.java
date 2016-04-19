package edu.nyu.tandon.query;

/*		 
 * MG4J: Managing Gigabytes for Java (big)
 *
 * Copyright (C) 2005-2015 Sebastiano Vigna 
 *
 *  This library is free software; you can redistribute it and/or modify it
 *  under the terms of the GNU Lesser General Public License as published by the Free
 *  Software Foundation; either version 3 of the License, or (at your option)
 *  any later version.
 *
 *  This library is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 *  or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 *  for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses/>.
 *
 */

import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.query.SelectedInterval;
import it.unimi.di.big.mg4j.query.nodes.Query;
import it.unimi.di.big.mg4j.query.nodes.QueryBuilderVisitor;
import it.unimi.di.big.mg4j.query.parser.QueryParser;
import it.unimi.di.big.mg4j.search.DocumentIterator;
import it.unimi.di.big.mg4j.search.score.*;
import it.unimi.dsi.fastutil.longs.LongSet;
import it.unimi.dsi.fastutil.objects.Object2ReferenceMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.Reference2DoubleMap;
import it.unimi.dsi.lang.FlyweightPrototype;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static it.unimi.di.big.mg4j.search.DocumentIterator.END_OF_LIST;


/**
 * An engine that takes a query and returns results, using a programmable
 * set of scorers and policies.
 * <p>
 * <p>This class embodies most of the work that must be done when answering a query.
 * Basically, {@link #process(String, int, int, ObjectArrayList) process(query,offset,length,results)} takes <code>query</code>,
 * parses it, turns it into a document iterator, scans the results, and deposits
 * <code>length</code> results starting at <code>offset</code> into the list <code>results</code>.
 * <p>
 * <p>There however several additional features available. First of all, either by separating
 * several queries with commas, or using directly {@link #process(Query[], int, int, ObjectArrayList)}
 * it is possible to resolve a series of queries with an &ldquo;and-then&rdquo; semantics: results
 * are added from each query, provided they did not appear before.
 * <p>
 * <p>It is possible to {@linkplain #score(Scorer[], double[]) score queries} using one or
 * more scorer with different weights (see {@link it.unimi.di.big.mg4j.search.score}), and also
 * set {@linkplain #setWeights(Reference2DoubleMap) different weights for different indices} (they
 * will be passed to the scorers). The scorers influence the order when processing each query,
 * but results from different &ldquo;and-then&rdquo; queries are simply concatenated.
 * <p>
 * <p>When using multiple scorers, <em>{@linkplain #equalize(int) equalisation}</em> can be used
 * to avoid the problem associated with the potentially different value ranges of each scorer. Equalisation
 * evaluates a settable number of sample documents and normalize the scorers using the maximum value in
 * the sample. See {@link AbstractAggregator} for some elaboration.
 * <p>
 * <p><em>{@linkplain #multiplex Multiplexing}</em> transforms a query <samp><var>q</var></samp> into <samp>index0:<var>q</var> | index1:<var>q</var> &hellip;</samp>.
 * In other words, the query is multiplexed on all available indices. Note that if inside <samp><var>q</var></samp>
 * there are selection operators that specify an index, the inner specification will overwrite
 * the external one, so that the semantics of the query is only amplified, but never contradicted.
 * <p>
 * <p>The results returned are instances of {@link DocumentScoreInfo}. If
 * an {@linkplain #intervalSelector interval selector} has been set,
 * the <code>info</code> field will contain a map from indices to arrays of {@linkplain SelectedInterval selected intervals}
 * satisfying the query (see {@link it.unimi.di.big.mg4j.search} for some elaboration on minimal-interval semantics support in MG4J).
 * <p>
 * <p>For examples of usage of this class, please look at {@link it.unimi.di.big.mg4j.query.Query}
 * and {@link it.unimi.di.big.mg4j.query.QueryServlet}.
 * <p>
 * <p><strong>Warning:</strong> This class is <strong>highly experimental</strong>. It has become
 * definitely more decent in MG4J, but still needs some refactoring.
 * <p>
 * <p><strong>Warning</strong>: This class is not
 * thread safe, but it provides {@linkplain FlyweightPrototype flyweight copies}.
 * The {@link #copy()} method is strengthened so to return an object implementing this interface.
 *
 * @author Sebastiano Vigna
 * @author Paolo Boldi
 * @since 1.0
 */

public class HitsQueryEngine<T> extends QueryEngine<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(HitsQueryEngine.class);

    /**
     * Creates a new query engine.
     *
     * @param queryParser    a query parser, or <code>null</code> if this query engine will {@linkplain #process(Query[], int, int, ObjectArrayList) just process pre-parsed queries}.
     * @param builderVisitor a builder visitor to transform {@linkplain Query queries} into {@linkplain DocumentIterator document iterators}.
     * @param indexMap       a map from symbolic name to indices (used for multiplexing and default weight initialisation).
     */
    public HitsQueryEngine(QueryParser queryParser, QueryBuilderVisitor<DocumentIterator> builderVisitor, Object2ReferenceMap<String, Index> indexMap) {
        super(queryParser, builderVisitor, indexMap);
    }

    @Override
    protected int getScoredResults(final DocumentIterator documentIterator, final int offset, final int length,
                                   final double lastMinScore, final ObjectArrayList<DocumentScoreInfo<T>> results, final LongSet alreadySeen) throws IOException {

        final ScoredDocumentBoundedSizeQueue<ObjectArrayList<Byte>> top = new ScoredDocumentBoundedSizeQueue<ObjectArrayList<Byte>>(offset + length);

        long document;
        int count = 0; // Number of not-already-seen documents

        scorer.wrap(documentIterator);
        // TODO: we should avoid enqueueing until we really know we shall use the values
        if (alreadySeen != null)
            while ((document = scorer.nextDocument()) != END_OF_LIST) {
                if (alreadySeen.add(document)) continue;
                count++;
                // save the terms involved in this score
                ObjectArrayList<Byte> termList = new ObjectArrayList<Byte>();
                for (int i = 0; i < ((BM25Scorer) scorer).flatIndexIterator.length; i++) {
                    if (document == ((BM25Scorer) scorer).flatIndexIterator[i].document()) {
                        termList.add((byte) i);
                    }
                }
                top.enqueue(document, scorer.score(), termList);
            }
        else
            while ((document = scorer.nextDocument()) != END_OF_LIST) {
                // save the terms involved in this score
                ObjectArrayList<Byte> termList = new ObjectArrayList<Byte>();
                for (int i = 0; i < ((BM25Scorer) scorer).flatIndexIterator.length; i++) {
                    if (document == ((BM25Scorer) scorer).flatIndexIterator[i].document()) {
                        termList.add((byte) i);
                    }
                }
                count++;
                top.enqueue(document, scorer.score(), termList);
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
}
