package edu.nyu.tandon.shard.ranking.taily;

import edu.nyu.tandon.test.BaseTest;
import it.unimi.di.big.mg4j.query.nodes.QueryBuilderVisitorException;
import it.unimi.di.big.mg4j.query.parser.QueryParserException;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class TailyShardSelectorTest extends BaseTest {

    public TailyShardSelector selector;

    @Before
    public void setUpSelector() throws Exception {
        String basename = buildCluster();
        StatisticalShardRepresentation representation = new StatisticalShardRepresentation(basename);
        representation.write(representation.calc());
        representation = new StatisticalShardRepresentation(basename + "-0");
        representation.write(representation.calc());
        representation = new StatisticalShardRepresentation(basename + "-1");
        representation.write(representation.calc());

        selector = new TailyShardSelector(basename, 2).withNc(1);
    }

    @Test
    public void noneOccur() throws Exception {
        assertThat(selector.selectShards("dog cat").size(), equalTo(0));
        assertThat(selector.shardScores("dog cat").values(), Matchers.contains(0.0, 0.0));
    }

    @Test
    public void oneOccurs() throws QueryParserException, QueryBuilderVisitorException, IOException {
        assertThat(selector.shardScores("dog a").values(), Matchers.contains(0.0, 0.0));
    }

    @Test
    public void allOccurOnlyInFirst() throws QueryParserException, QueryBuilderVisitorException, IOException {
        assertThat(selector.shardScores("a b").values(), Matchers.contains(0.8571428571428572, 0.0));
    }

    @Test
    public void allOccurOnlyInSecond() throws QueryParserException, QueryBuilderVisitorException, IOException {
        assertThat(selector.shardScores("w").values(), Matchers.contains(0.0, 1.0));
    }
}
