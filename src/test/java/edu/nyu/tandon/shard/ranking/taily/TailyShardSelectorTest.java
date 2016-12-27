package edu.nyu.tandon.shard.ranking.taily;

import edu.nyu.tandon.test.BaseTest;
import it.unimi.di.big.mg4j.query.nodes.QueryBuilderVisitorException;
import it.unimi.di.big.mg4j.query.parser.QueryParserException;
import org.apache.commons.configuration.ConfigurationException;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class TailyShardSelectorTest extends BaseTest {

    @Test
    public void test() throws IllegalAccessException, URISyntaxException, IOException, InstantiationException, NoSuchMethodException, ConfigurationException, InvocationTargetException, ClassNotFoundException, QueryParserException, QueryBuilderVisitorException {
        // given
        String basename = buildIndexA();
        StatisticalShardRepresentation representation = new StatisticalShardRepresentation(basename);
        representation.write(representation.calc());

        // when
//        TailyShardSelector selector = new TailyShardSelector(clusterDir.getAbsolutePath() + "/gov2C", 11);

        // then
//        selector.selectShards("dog cat");
    }
}
