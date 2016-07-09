package edu.nyu.tandon.query;

import edu.nyu.tandon.test.BaseTest;
import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.index.TermProcessor;
import it.unimi.di.big.mg4j.index.cluster.SelectiveQueryEngine;
import it.unimi.di.big.mg4j.query.SelectedInterval;
import it.unimi.di.big.mg4j.query.nodes.QueryBuilderVisitorException;
import it.unimi.di.big.mg4j.query.parser.QueryParserException;
import it.unimi.di.big.mg4j.query.parser.SimpleParser;
import it.unimi.di.big.mg4j.search.DocumentIteratorBuilderVisitor;
import it.unimi.di.big.mg4j.search.score.DocumentScoreInfo;
import it.unimi.dsi.fastutil.Hash;
import it.unimi.dsi.fastutil.objects.*;
import org.apache.commons.configuration.ConfigurationException;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;

import static edu.nyu.tandon.query.Query.MAX_STEMMING;
import static java.util.Arrays.sort;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class SelectiveQueryEngineTest extends BaseTest {

    protected static SelectiveQueryEngine<Reference2ObjectMap<Index, SelectedInterval[]>> selectiveQueryEngine;

    @BeforeClass
    public static void loadEngine() throws IllegalAccessException, InvocationTargetException, NoSuchMethodException, IOException, InstantiationException, URISyntaxException, ConfigurationException, ClassNotFoundException {

//        final Object2ReferenceLinkedOpenHashMap<String, Index> indexMap = new Object2ReferenceLinkedOpenHashMap<String, Index>(Hash.DEFAULT_INITIAL_SIZE, .5f);
//        final Reference2DoubleOpenHashMap<Index> index2Weight = new Reference2DoubleOpenHashMap<Index>();
//        Query.loadIndicesFromSpec(
//                new String[] { getFileFromResourcePath("clusters").getAbsolutePath() + "/gov2C" },
//                true, null, indexMap, index2Weight);
//
//        final Reference2ReferenceMap<Index, Object> index2Parser = new Reference2ReferenceOpenHashMap<Index, Object>();
//        final Object2ObjectOpenHashMap<String, TermProcessor> termProcessors = new Object2ObjectOpenHashMap<String, TermProcessor>(indexMap.size());
//        for (String alias : indexMap.keySet()) termProcessors.put(alias, indexMap.get(alias).termProcessor);
//        final SimpleParser simpleParser = new SimpleParser(indexMap.keySet(), indexMap.firstKey(), termProcessors);
//        selectiveQueryEngine = new SelectiveQueryEngine<Reference2ObjectMap<Index, SelectedInterval[]>>(
//                simpleParser,
//                new DocumentIteratorBuilderVisitor(indexMap, index2Parser, indexMap.get(indexMap.firstKey()), MAX_STEMMING),
//                indexMap,
//                getFileFromResourcePath("clusters").getAbsolutePath() + "/gov2C",
//                getFileFromResourcePath("csi").getAbsolutePath() + "/csi"
//        );
    }

//    @Test
//    public void resolveClusterBasenames() {
//        // Given
//        String directory = getFileFromResourcePath("clusters").getAbsolutePath();
//        String basename = directory + "/gov2C";
//
//        // When
//        String[] actualClusterBasenames = selectiveQueryEngine.resolveClusterBasenames(basename);
//        sort(actualClusterBasenames);
//
//        // Then
//        assertThat(actualClusterBasenames, equalTo(new String[] {
//                directory + "/gov2C-0",
//                directory + "/gov2C-1",
//                directory + "/gov2C-10",
//                directory + "/gov2C-2",
//                directory + "/gov2C-3",
//                directory + "/gov2C-4",
//                directory + "/gov2C-5",
//                directory + "/gov2C-6",
//                directory + "/gov2C-7",
//                directory + "/gov2C-8",
//                directory + "/gov2C-9"
//        }));
//    }

    @Test
    public void processQuery() throws QueryParserException, QueryBuilderVisitorException, IOException {
        ObjectArrayList<DocumentScoreInfo<Reference2ObjectMap<Index, SelectedInterval[]>>> results =
                new ObjectArrayList<>();
        selectiveQueryEngine.process("us oil industry", 0, 10, results);
    }

}
