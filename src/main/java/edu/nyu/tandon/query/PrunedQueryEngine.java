package edu.nyu.tandon.query;

import edu.nyu.tandon.search.score.BM25PrunedScorer;
import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.query.SelectedInterval;
import it.unimi.di.big.mg4j.query.nodes.QueryBuilderVisitor;
import it.unimi.di.big.mg4j.query.parser.QueryParser;
import it.unimi.di.big.mg4j.search.DocumentIterator;
import it.unimi.di.big.mg4j.search.score.DocumentScoreInfo;
import it.unimi.di.big.mg4j.search.score.ScoredDocumentBoundedSizeQueue;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongSet;
import it.unimi.dsi.fastutil.objects.Object2ReferenceMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.Reference2ObjectMap;
import it.unimi.dsi.io.InputBitStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.HashSet;
import java.util.Set;
import java.util.function.BiConsumer;

import static it.unimi.di.big.mg4j.search.DocumentIterator.END_OF_LIST;

// TODO: do not currently support multiple indeces + weights spec in the index name
public class PrunedQueryEngine<T> extends QueryEngine<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(PrunedQueryEngine.class);

    public boolean prunedIndex;                     // index is pruned
    public boolean docPrunning;                     // use document pruning
    public Set<Long> DocumentList;                  // document pruning
    public LongArrayList globalTermFrequencies;     // posting pruning

    public PrunedQueryEngine(final QueryParser queryParser, final QueryBuilderVisitor<DocumentIterator> builderVisitor, final Object2ReferenceMap<String, Index> indexMap) {
        super(queryParser, builderVisitor, indexMap);
        this.DocumentList = new HashSet<Long>();
        this.globalTermFrequencies = new LongArrayList();
        prunedIndex = false;
    }

    /** load the document ID list for document pruned index
     *
      * @param basename     filename of the sorted document list
     * @param threshod      the number of document IDs to load
     * @return              this object
     * @throws Exception
     */
    public PrunedQueryEngine<T> loadDocumentPrunedList(final String basename, final int threshod) throws Exception {
        DocumentList.clear();
        String line;
        BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(basename), Charset.forName("UTF-8")));
        int n = 0;
        while ((line = br.readLine()) != null) {
            DocumentList.add(Long.parseLong(line));
            if (n++ > threshod) break;
        }
        return this;
    }

    /** load the global term frequencies used for scoring in a posting pruned index
     *
     * @param basename      filename for gamma encoded term frequencies
     * @return              this object
     * @throws Exception    IOException
     */
    public PrunedQueryEngine<T> loadGlobalTermFrequencies(final String basename) throws Exception {
        globalTermFrequencies.clear();
        globalTermFrequencies.trim();
        String line;

        InputBitStream tf = new InputBitStream(basename);
        int n = 0;
        while (tf.hasNext()) {
            try {
                globalTermFrequencies.add(tf.readLongGamma());
            } catch (IOException ex) {
                break;
            }
        }
        prunedIndex = true;
        return this;
    }

    protected boolean isDocumentPruned(long docID) {
        boolean res = false;
        res = !this.docPrunning || this.DocumentList.contains(docID);
        return res;
    }

    @Override
    protected int getScoredResults(final DocumentIterator documentIterator, final int offset, final int length, final double lastMinScore, final ObjectArrayList<DocumentScoreInfo<T>> results, final LongSet alreadySeen) throws IOException {

        final ScoredDocumentBoundedSizeQueue<Reference2ObjectMap<Index, SelectedInterval[]>> top = new ScoredDocumentBoundedSizeQueue<Reference2ObjectMap<Index, SelectedInterval[]>>(offset + length);
        long document;
        int count = 0; // Number of not-already-seen documents

        System.out.print(".");

        scorer.wrap(documentIterator);

        // TODO: we should avoid enqueueing until we really know we shall use the values
        if (alreadySeen != null)
            while ((document = scorer.nextDocument()) != END_OF_LIST) {
                if (alreadySeen.add(document)) continue;
                if (!isDocumentPruned(document)) continue;
                count++;
                top.enqueue(document, scorer.score());
            }
        else
            while ((document = scorer.nextDocument()) != END_OF_LIST) {
                if (!isDocumentPruned(document)) continue;
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

        System.out.print(".");

        // Unfortunately, to provide the exact count of results we have to scan the whole iterator.
        if (alreadySeen != null)
            while ((document = documentIterator.nextDocument()) != END_OF_LIST) {
                if (!alreadySeen.add(document)) continue;
                if (!isDocumentPruned(document)) continue;
                if (count >= offset && count < offset + length) results.add(new DocumentScoreInfo<T>(document, -1));
                count++;
            }
        else if (length != 0)
            while ((document = documentIterator.nextDocument()) != END_OF_LIST) {
                if (!isDocumentPruned(document)) continue;
                if (count < offset + length && count >= offset) results.add(new DocumentScoreInfo<T>(document, -1));
                count++;
            }
        else while ((document = documentIterator.nextDocument()) != END_OF_LIST) count++;

        return count;
    }


}
