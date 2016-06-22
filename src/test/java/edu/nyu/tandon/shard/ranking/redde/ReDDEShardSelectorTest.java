package edu.nyu.tandon.shard.ranking.redde;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import edu.nyu.tandon.shard.csi.Result;
import edu.nyu.tandon.test.BaseTest;
import it.unimi.di.big.mg4j.query.nodes.QueryBuilderVisitorException;
import it.unimi.di.big.mg4j.query.parser.QueryParserException;
import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;
import org.apache.commons.configuration.ConfigurationException;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class ReDDEShardSelectorTest extends BaseTest {

    protected static ReDDEShardSelector reddeSelector;

    @BeforeClass
    public static void initSelector() throws IllegalAccessException, InvocationTargetException, NoSuchMethodException, IOException, InstantiationException, URISyntaxException, ConfigurationException, ClassNotFoundException {
        reddeSelector = new ReDDEShardSelector(loadCSI());
    }

    @Test
    public void computeSampleSizes() throws IllegalAccessException, InvocationTargetException, NoSuchMethodException, IOException, InstantiationException, URISyntaxException, ConfigurationException, ClassNotFoundException {
        // Given
        ReDDEShardSelector selector = reddeSelector;

        // When
        Int2LongOpenHashMap sampleSizes = selector.computeSampleSizes();

        // Then
        assertThat(sampleSizes, equalTo(new Int2LongOpenHashMap(
                new int[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 },
                new long[] { 10, 10, 11, 10, 15, 6, 10, 9, 7, 13, 8 })));
    }

    @Test
    public void computeShardCounts() {
        // Given
        ReDDEShardSelector selector = reddeSelector;
        List<Result> results = Arrays.asList(new Result[] {
                new Result(1, 1., 1),
                new Result(2, 1., 1),
                new Result(3, 1., 1),
                new Result(4, 1., 2),
                new Result(5, 1., 2),
                new Result(6, 1., 3)
        });

        // When
        Map<Integer, Long> actualShardCounts = selector.computeShardCounts(results);

        // Then
        assertThat(actualShardCounts, equalTo(ImmutableMap.of(
                1, 3l,
                2, 2l,
                3, 1l
        )));
    }

    @Test
    public void shardWeight() {
        // Given
        ReDDEShardSelector selector = reddeSelector;
        selector.csi = spy(selector.csi);
        when(selector.csi.numberOfDocuments(0)).thenReturn(10l);
        selector.sampleSizes = new Int2LongOpenHashMap(
                new int[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 },
                new long[] { 10, 7, 14, 13, 6, 9, 11, 6, 11, 13, 9 });

        // When
        double actualWeight = selector.shardWeight(0);

        // Then
        assertThat(actualWeight, equalTo(1.));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shardWeightThrowException() {
        // Given
        ReDDEShardSelector selector = reddeSelector;
        selector.sampleSizes = new Int2LongOpenHashMap(
                new int[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 },
                new long[] { 10, 7, 14, 13, 6, 9, 11, 6, 11, 13, 9 });

        // When
        selector.shardWeight(11);
    }

    @Test
    public void computeShardScores() {
        // Given
        ReDDEShardSelector selector = spy(reddeSelector);
        doReturn(1.).when(selector).computeShardScore(eq(0), anyLong());
        doReturn(2.).when(selector).computeShardScore(eq(1), anyLong());
        doReturn(3.).when(selector).computeShardScore(eq(2), anyLong());

        // When
        Map<Integer, Double> actualShardScores = selector.computeShardScores(ImmutableMap.of(
                0, 0l,
                1, 0l,
                2, 0l
        ));

        // Then
        assertThat(actualShardScores, equalTo(ImmutableMap.of(
                0, 1.,
                1, 2.,
                2, 3.
        )));
    }

    @Test
    public void selectShards() throws QueryParserException, QueryBuilderVisitorException, IOException {
        // Given
        ReDDEShardSelector selector = spy(reddeSelector);
        selector.withT(5);
        doReturn(ImmutableMap.builder()
                .put(1, 0.1)
                .put(2, 1.0)
                .put(3, 0.5)
                .put(4, 2.1)
                .put(5, 3.0)
                .put(6, 0.9)
                .put(7, 1.1)
                .put(8, 0.2)
                .put(9, 10.0)
                .put(10, 4.1)
                .build()
        ).when(selector).computeShardScores(anyMap());

        // When
        List<Integer> actualShards = selector.selectShards("QUERY");

        // Then
        assertThat(actualShards, equalTo(ImmutableList.of(9, 10, 5, 4, 7)));
    }

}
