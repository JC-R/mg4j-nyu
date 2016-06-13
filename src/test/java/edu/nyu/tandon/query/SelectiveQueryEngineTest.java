package edu.nyu.tandon.query;

import com.google.common.collect.ImmutableList;
import edu.nyu.tandon.experiments.logger.EventLogger;
import edu.nyu.tandon.experiments.querylistener.QueryListener;
import edu.nyu.tandon.test.BaseTest;
import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.query.SelectedInterval;
import it.unimi.di.big.mg4j.query.nodes.QueryBuilderVisitorException;
import it.unimi.di.big.mg4j.query.parser.QueryParserException;
import it.unimi.di.big.mg4j.search.score.DocumentScoreInfo;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.Reference2ObjectMap;
import org.apache.commons.configuration.ConfigurationException;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

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
        selectiveQueryEngine = new SelectiveQueryEngine<>(
                getFileFromResourcePath("clusters").getAbsolutePath() + "/gov2C",
                getFileFromResourcePath("csi").getAbsolutePath() + "/csi"
        );
    }

    @Test
    public void resolveClusterBasenames() {
        // Given
        String directory = getFileFromResourcePath("clusters").getAbsolutePath();
        String basename = directory + "/gov2C";

        // When
        String[] actualClusterBasenames = selectiveQueryEngine.resolveClusterBasenames(basename);
        sort(actualClusterBasenames);

        // Then
        assertThat(actualClusterBasenames, equalTo(new String[] {
                directory + "/gov2C-0",
                directory + "/gov2C-1",
                directory + "/gov2C-10",
                directory + "/gov2C-2",
                directory + "/gov2C-3",
                directory + "/gov2C-4",
                directory + "/gov2C-5",
                directory + "/gov2C-6",
                directory + "/gov2C-7",
                directory + "/gov2C-8",
                directory + "/gov2C-9"
        }));
    }

//    @Test
//    public void queryListener() throws QueryParserException, QueryBuilderVisitorException, IOException {
//        // Given
//        final List<String> l = new ArrayList<>();
//        selectiveQueryEngine.addEventLogger(new EventLogger() {
//
//            @Override
//            public void onStart(Object... o) {
//
//            }
//
//            @Override
//            public void onEnd(Object... o) {
//
//            }
//        });
//
//        // When
//        final ObjectArrayList<DocumentScoreInfo<Reference2ObjectMap<Index, SelectedInterval[]>>> results = new ObjectArrayList<>();
//        selectiveQueryEngine.process("queryA", 0, 10, results);
//        selectiveQueryEngine.process("queryB", 0, 10, results);
//        selectiveQueryEngine.process("queryC", 0, 10, results);
//
//        // Then
//        assertThat(l, equalTo(ImmutableList.of(
//                "START:queryA", "END:queryA",
//                "START:queryB", "END:queryB",
//                "START:queryC", "END:queryC"
//        )));
//    }

}
