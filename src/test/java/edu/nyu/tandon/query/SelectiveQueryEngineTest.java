package edu.nyu.tandon.query;

import edu.nyu.tandon.search.score.BM25PrunedScorer;
import edu.nyu.tandon.search.score.QueryLikelihoodScorer;
import edu.nyu.tandon.test.BaseTest;
import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.index.TermProcessor;
import it.unimi.di.big.mg4j.index.cluster.SelectiveQueryEngine;
import it.unimi.di.big.mg4j.query.SelectedInterval;
import it.unimi.di.big.mg4j.query.nodes.QueryBuilderVisitorException;
import it.unimi.di.big.mg4j.query.parser.QueryParserException;
import it.unimi.di.big.mg4j.query.parser.SimpleParser;
import it.unimi.di.big.mg4j.search.DocumentIteratorBuilderVisitor;
import it.unimi.di.big.mg4j.search.score.BM25Scorer;
import it.unimi.di.big.mg4j.search.score.DocumentScoreInfo;
import it.unimi.dsi.fastutil.Hash;
import it.unimi.dsi.fastutil.objects.*;
import org.apache.commons.configuration.ConfigurationException;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;

import static edu.nyu.tandon.query.Query.MAX_STEMMING;
import static java.util.Arrays.sort;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class SelectiveQueryEngineTest extends BaseTest {

    protected static SelectiveQueryEngine<Reference2ObjectMap<Index, SelectedInterval[]>> selectiveQueryEngine;

    @Before
    public void loadEngine() throws IllegalAccessException, InvocationTargetException, NoSuchMethodException, IOException, InstantiationException, URISyntaxException, ConfigurationException, ClassNotFoundException {

        final Object2ReferenceLinkedOpenHashMap<String, Index> indexMap = new Object2ReferenceLinkedOpenHashMap<String, Index>(Hash.DEFAULT_INITIAL_SIZE, .5f);
        final Reference2DoubleOpenHashMap<Index> index2Weight = new Reference2DoubleOpenHashMap<Index>();
        Query.loadIndicesFromSpec(
                new String[] { getFileFromResourcePath("clusters").getAbsolutePath() + "/gov2C" },
                true, null, indexMap, index2Weight);

        final Reference2ReferenceMap<Index, Object> index2Parser = new Reference2ReferenceOpenHashMap<Index, Object>();
        final Object2ObjectOpenHashMap<String, TermProcessor> termProcessors = new Object2ObjectOpenHashMap<String, TermProcessor>(indexMap.size());
        for (String alias : indexMap.keySet()) termProcessors.put(alias, indexMap.get(alias).termProcessor);
        final SimpleParser simpleParser = new SimpleParser(indexMap.keySet(), indexMap.firstKey(), termProcessors);
        selectiveQueryEngine = new SelectiveQueryEngine<Reference2ObjectMap<Index, SelectedInterval[]>>(
                simpleParser,
                new DocumentIteratorBuilderVisitor(indexMap, index2Parser, indexMap.get(indexMap.firstKey()), MAX_STEMMING),
                indexMap,
                getFileFromResourcePath("clusters").getAbsolutePath() + "/gov2C",
                getFileFromResourcePath("csi").getAbsolutePath() + "/csi"
        );
    }

    @Test
    public void processQuery() throws QueryParserException, QueryBuilderVisitorException, IOException {
        ObjectArrayList<DocumentScoreInfo<Reference2ObjectMap<Index, SelectedInterval[]>>> results =
                new ObjectArrayList<>();
        selectiveQueryEngine.process("us oil industry", 0, 10, results);
    }

    @Test
    public void processQueryWithQueryLikelihoodScorer() throws QueryParserException, QueryBuilderVisitorException, IOException {
        ObjectArrayList<DocumentScoreInfo<Reference2ObjectMap<Index, SelectedInterval[]>>> results =
                new ObjectArrayList<>();
        selectiveQueryEngine.score(new QueryLikelihoodScorer());
        selectiveQueryEngine.process("us oil industry", 0, 10, results);
    }

    @Test
    public void sameAsGlobalShortConjuctive() throws QueryParserException, QueryBuilderVisitorException, IOException, IllegalAccessException, URISyntaxException, InstantiationException, ConfigurationException, NoSuchMethodException, InvocationTargetException, ClassNotFoundException {
        sameAsGlobal("us oil industry");
    }

    @Test
    public void sameAsGlobalLongDisjunctive() throws QueryParserException, QueryBuilderVisitorException, IOException, IllegalAccessException, URISyntaxException, InstantiationException, ConfigurationException, NoSuchMethodException, InvocationTargetException, ClassNotFoundException {
        sameAsGlobal("the OR greatest OR threat OR to OR the OR existence OR of OR the OR bald OR eagle OR came OR from OR the OR extensive OR use OR of OR ddt OR and OR other OR pesticides OR after OR world OR war OR ii");
    }

    @Test
    public void sameAsGlobalTree() throws QueryParserException, QueryBuilderVisitorException, IOException, IllegalAccessException, URISyntaxException, InstantiationException, ConfigurationException, NoSuchMethodException, InvocationTargetException, ClassNotFoundException {
        sameAsGlobal("the OR (greatest OR threat)");
    }

    public void sameAsGlobal(String query) throws QueryParserException, QueryBuilderVisitorException, IOException, IllegalAccessException, URISyntaxException, InstantiationException, ConfigurationException, NoSuchMethodException, InvocationTargetException, ClassNotFoundException {
        ObjectArrayList<DocumentScoreInfo<Reference2ObjectMap<Index, SelectedInterval[]>>> results =
                new ObjectArrayList<>();
        selectiveQueryEngine.score(new BM25PrunedScorer());
        selectiveQueryEngine.process(query, 0, 10, results);

        ObjectArrayList<DocumentScoreInfo<Reference2ObjectMap<Index, SelectedInterval[]>>> globalResults =
                new ObjectArrayList<>();
        globalEngine().process(query, 0, 10, globalResults);

        assertThat(results.size(), equalTo(globalResults.size()));
        for (int i = 0; i < results.size(); i++) {
            assertThat(results.get(i).document, equalTo(globalResults.get(i).document));
            assertEquals(globalResults.get(i).score, results.get(i).score, 0.0000000000001);
        }
    }

    public QueryEngine globalEngine() throws IllegalAccessException, InvocationTargetException, NoSuchMethodException, IOException, InstantiationException, URISyntaxException, ConfigurationException, ClassNotFoundException {

        final Object2ReferenceLinkedOpenHashMap<String, Index> indexMap = new Object2ReferenceLinkedOpenHashMap<String, Index>(Hash.DEFAULT_INITIAL_SIZE, .5f);
        final Reference2DoubleOpenHashMap<Index> index2Weight = new Reference2DoubleOpenHashMap<Index>();
        Query.loadIndicesFromSpec(
                new String[] { getFileFromResourcePath("index").getAbsolutePath() + "/gov2-text" },
                true, null, indexMap, index2Weight);

        final Reference2ReferenceMap<Index, Object> index2Parser = new Reference2ReferenceOpenHashMap<Index, Object>();
        final Object2ObjectOpenHashMap<String, TermProcessor> termProcessors = new Object2ObjectOpenHashMap<String, TermProcessor>(indexMap.size());
        for (String alias : indexMap.keySet()) termProcessors.put(alias, indexMap.get(alias).termProcessor);
        final SimpleParser simpleParser = new SimpleParser(indexMap.keySet(), indexMap.firstKey(), termProcessors);
        final QueryEngine queryEngine = new QueryEngine(simpleParser, new DocumentIteratorBuilderVisitor(indexMap, index2Parser, indexMap.get(indexMap.firstKey()), MAX_STEMMING), indexMap);
        queryEngine.score(new BM25Scorer());

        return queryEngine;
    }

}
