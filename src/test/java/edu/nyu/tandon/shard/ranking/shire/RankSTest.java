package edu.nyu.tandon.shard.ranking.shire;

import edu.nyu.tandon.shard.csi.Result;
import edu.nyu.tandon.shard.ranking.shire.node.Document;
import edu.nyu.tandon.shard.ranking.shire.node.Intermediate;
import edu.nyu.tandon.shard.ranking.shire.node.Node;
import edu.nyu.tandon.test.BaseTest;
import org.apache.commons.configuration.ConfigurationException;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class RankSTest extends BaseTest {

    @Test
    public void transform() throws IllegalAccessException, InvocationTargetException, NoSuchMethodException, IOException, InstantiationException, URISyntaxException, ConfigurationException, ClassNotFoundException {

        // Given
        RankS ranker = new RankS(loadCSI(), 2);
        List<Result> results = Arrays.asList(new Result[] {
                new Result(1, 0.9, 1),
                new Result(2, 0.8, 2),
                new Result(3, 0.7, 3),
                new Result(4, 0.6, 4)
        });

        // When
        Node actualTopRanked = ranker.transform(results);

        // Then
        Node expected = new Intermediate(
                new Intermediate(
                        new Intermediate(
                                new Document(1, 0.9),
                                new Document(2, 0.8)),
                        new Document(3, 0.7)),
                new Document(4, 0.6));
        Assert.assertThat(actualTopRanked.getParent().getParent().getParent(), CoreMatchers.equalTo(expected));

    }

}
