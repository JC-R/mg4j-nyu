package edu.nyu.tandon.query;

import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.query.nodes.QueryBuilderVisitor;
import it.unimi.di.big.mg4j.query.parser.QueryParser;
import it.unimi.di.big.mg4j.search.DocumentIterator;
import it.unimi.di.big.mg4j.search.score.DocumentScoreInfo;
import it.unimi.dsi.fastutil.longs.LongSet;
import it.unimi.dsi.fastutil.objects.Object2ReferenceMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static it.unimi.di.big.mg4j.search.DocumentIterator.END_OF_LIST;

/**
 * Created by juan on 2/21/16.
 */
public class FeaturesQueryEngine<T> extends QueryEngine<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(FeaturesQueryEngine.class);

    public FeaturesQueryEngine(final QueryParser queryParser, final QueryBuilderVisitor<DocumentIterator> builderVisitor, final Object2ReferenceMap<String, Index> indexMap) {
        super(queryParser, builderVisitor, indexMap);
    }

    @Override
    protected int getScoredResults(final DocumentIterator documentIterator, final int offset, final int length, final double lastMinScore, final ObjectArrayList<DocumentScoreInfo<T>> results, final LongSet alreadySeen) throws IOException {

        // we just visit every term list
        int count = 0;
        long document;
        System.out.print(".");
        scorer.wrap(documentIterator);
        while ((document = scorer.nextDocument()) != END_OF_LIST) {
            count++;
            // output feature
//            ( term, document,scorer.score() );
        }
        return count;
    }

    @Override
    protected int getResults(final DocumentIterator documentIterator, final int offset, final int length, final ObjectArrayList<DocumentScoreInfo<T>> results, final LongSet alreadySeen) throws IOException {

        int count = 0;
        long document;
        System.out.print(".");
        while ((document = documentIterator.nextDocument()) != END_OF_LIST) {
            count++;
            // output feature
//          ( document, scorer.score() );
        }
        return count;
    }


}
