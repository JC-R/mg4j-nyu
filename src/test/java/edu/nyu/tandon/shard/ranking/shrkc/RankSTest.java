package edu.nyu.tandon.shard.ranking.shrkc;

import com.google.common.collect.ImmutableMap;
import edu.nyu.tandon.shard.csi.CentralSampleIndex;
import edu.nyu.tandon.shard.csi.Result;
import edu.nyu.tandon.shard.ranking.shrkc.node.Document;
import edu.nyu.tandon.shard.ranking.shrkc.node.Intermediate;
import edu.nyu.tandon.shard.ranking.shrkc.node.Node;
import edu.nyu.tandon.test.BaseTest;
import it.unimi.di.big.mg4j.query.nodes.QueryBuilderVisitorException;
import it.unimi.di.big.mg4j.query.parser.QueryParserException;
import org.apache.commons.configuration.ConfigurationException;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class RankSTest extends BaseTest {

    @Test
    public void transform() throws IllegalAccessException, InvocationTargetException, NoSuchMethodException, IOException, InstantiationException, URISyntaxException, ConfigurationException, ClassNotFoundException, QueryParserException, QueryBuilderVisitorException {

        // Given
        RankS ranker = new RankS(loadCSI(), 2);
        ranker.csi = mock(CentralSampleIndex.class);
        doReturn(Arrays.asList(
                new Result(1, 0.9, 1),
                new Result(2, 0.8, 2),
                new Result(3, 0.7, 3),
                new Result(4, 0.6, 4)
        )).when(ranker.csi).runQuery(any());

        // When
        Map<Integer, Double> scores = ranker.shardScores("");

        // Then
        Assert.assertThat(scores, CoreMatchers.equalTo(
                ImmutableMap.of(
                        1, 0.9,
                        2, 0.8 * Math.pow(2, -1),
                        3, 0.7 * Math.pow(2, -2),
                        4, 0.6 * Math.pow(2, -3)
                )
        ));

    }

}
