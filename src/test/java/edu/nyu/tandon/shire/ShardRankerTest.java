package edu.nyu.tandon.shire;

import edu.nyu.tandon.csi.Result;
import edu.nyu.tandon.shire.node.Document;
import edu.nyu.tandon.shire.node.Intermediate;
import edu.nyu.tandon.shire.node.Node;
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
public class ShardRankerTest extends BaseTest {

    @Test
    public void traverseOneDocumentTree() throws IllegalAccessException, InvocationTargetException, NoSuchMethodException, IOException, InstantiationException, URISyntaxException, ConfigurationException, ClassNotFoundException {
        Node topRanked = new Document(1);
        traverseTree(topRanked, Arrays.asList(new Integer[] { 1 }));
    }

    @Test
    public void traverseTwoDocumentTree() throws IllegalAccessException, InvocationTargetException, NoSuchMethodException, IOException, InstantiationException, URISyntaxException, ConfigurationException, ClassNotFoundException {
        Node topRanked = new Document(1);
        Node root = new Intermediate(topRanked, new Document(2));
        traverseTree(topRanked, Arrays.asList(new Integer[] { 1, 2 }));
    }

    @Test
    public void traverseThreeDocumentTree() throws IllegalAccessException, InvocationTargetException, NoSuchMethodException, IOException, InstantiationException, URISyntaxException, ConfigurationException, ClassNotFoundException {
        Node topRanked = new Document(1);
        Node root = new Intermediate(new Intermediate(topRanked, new Document(2)), new Document(3));
        traverseTree(topRanked, Arrays.asList(new Integer[] { 1, 2, 3 }));
    }

    @Test
    public void traverseTreeWithSecondBetter() throws IllegalAccessException, InvocationTargetException, NoSuchMethodException, IOException, InstantiationException, URISyntaxException, ConfigurationException, ClassNotFoundException {
        Node topRanked = new Document(1);
        Node root = new Intermediate(new Intermediate(topRanked, new Document(2)), new Document(2));
        traverseTree(topRanked, Arrays.asList(new Integer[] { 2, 1 }));
    }

    public void traverseTree(Node topRanked, List<Integer> expected) throws IllegalAccessException, InvocationTargetException, NoSuchMethodException, IOException, InstantiationException, URISyntaxException, ConfigurationException, ClassNotFoundException {

        // Given
        ShardRanker shardRanker = new ShardRanker(loadCSI(), 1.5) {
            @Override
            protected Node transform(List<Result> results) {
                return null;
            }
        };

        // When
        List<Integer> actual = shardRanker.traverseTree(topRanked);

        // Then
        Assert.assertThat(actual, CoreMatchers.equalTo(expected));
    }

}
