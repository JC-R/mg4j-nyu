package edu.nyu.tandon.shard.csi;

import edu.nyu.tandon.index.cluster.SelectiveDocumentalIndexStrategy;
import edu.nyu.tandon.test.BaseTest;
import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.query.SelectedInterval;
import it.unimi.di.big.mg4j.query.nodes.QueryBuilderVisitorException;
import it.unimi.di.big.mg4j.query.parser.QueryParserException;
import it.unimi.di.big.mg4j.search.score.DocumentScoreInfo;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.objects.Object2LongFunction;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.Reference2ObjectMap;
import it.unimi.dsi.sux4j.mph.MWHCFunction;
import org.apache.commons.configuration.ConfigurationException;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.util.List;

import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class CentralSampleIndexTest extends BaseTest {

    protected static CentralSampleIndex csi;

    @BeforeClass
    public static void beforeClass() throws IllegalAccessException, URISyntaxException, IOException, InstantiationException, NoSuchMethodException, ConfigurationException, InvocationTargetException, ClassNotFoundException {
        csi = loadCSI();
    }

    @Test
    public void resolveCluster() throws IOException {

        assertThat(csi.resolveCluster(0l), equalTo(1));
        assertThat(csi.resolveCluster(1l), equalTo(7));
        assertThat(csi.resolveCluster(2l), equalTo(2));
    }

    @Test
    public void getResults() {

        // Given
        final ObjectArrayList<DocumentScoreInfo<Reference2ObjectMap<Index, SelectedInterval[]>>> r = new ObjectArrayList<DocumentScoreInfo<Reference2ObjectMap<Index, SelectedInterval[]>>>();
        r.add(new DocumentScoreInfo<Reference2ObjectMap<Index, SelectedInterval[]>>(1, 0.5));
        r.add(new DocumentScoreInfo<Reference2ObjectMap<Index, SelectedInterval[]>>(2, 0.4));
        r.add(new DocumentScoreInfo<Reference2ObjectMap<Index, SelectedInterval[]>>(3, 0.3));

        CentralSampleIndex spy = spy(csi);
        when(spy.resolveCluster(1)).thenReturn(1);
        when(spy.resolveCluster(2)).thenReturn(2);
        when(spy.resolveCluster(3)).thenReturn(3);

        // When
        List<Result> actual = spy.getResults(r);

        // Then
        assertThat(actual, equalTo(asList(
                new Result(1, 0.5, 1),
                new Result(2, 0.4, 2),
                new Result(3, 0.3, 3)
        )));
    }

    @Test
    public void runQueryAndNotFail() throws QueryParserException, QueryBuilderVisitorException, IOException, ClassNotFoundException {
        csi.runQuery("oil");
    }

}
