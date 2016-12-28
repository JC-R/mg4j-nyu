package edu.nyu.tandon.shard.ranking.taily;

import edu.nyu.tandon.shard.ranking.taily.StatisticalShardRepresentation.TermIterator;
import edu.nyu.tandon.shard.ranking.taily.StatisticalShardRepresentation.TermStats;
import edu.nyu.tandon.test.BaseTest;
import it.unimi.di.big.mg4j.index.IndexIterator;
import org.apache.commons.configuration.ConfigurationException;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;

import static it.unimi.di.big.mg4j.search.DocumentIterator.END_OF_LIST;
import static junit.framework.TestCase.assertEquals;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.core.IsNot.not;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class StatisticalShardRepresentationTest extends BaseTest {

    @Test
    public void termStats() throws IOException, IllegalAccessException, URISyntaxException, InstantiationException, ConfigurationException, NoSuchMethodException, InvocationTargetException, ClassNotFoundException {
        // given
        IndexIterator indexIterator = mock(IndexIterator.class);
        when(indexIterator.nextDocument())
                .thenReturn(1L)
                .thenReturn(2L)
                .thenReturn(END_OF_LIST);
        when(indexIterator.count())
                .thenReturn(2)
                .thenReturn(4);
        when(indexIterator.frequency()).thenReturn(6L);

        StatisticalShardRepresentation representation = spy(new StatisticalShardRepresentation("dummy"));
        doReturn(10L).when(representation).occurrency(eq(indexIterator));
        doReturn(100L).when(representation).collectionSize(eq(indexIterator));
        doReturn(10L).when(representation).documentSize(eq(indexIterator));
        representation.mu = 10.0;

        // when
        TermStats actual = representation.termStats(indexIterator);

        // then
        assertEquals(actual.expectedValue, -0.547235724334295, 0.000001);
        assertEquals(actual.variance, 0.620679110800021, 0.000001);
        assertEquals(actual.minValue, -1.897119984885881, 0.000001);
    }

    @Test
    public void termStatsArray() throws IOException, IllegalAccessException, URISyntaxException, InstantiationException, ConfigurationException, NoSuchMethodException, InvocationTargetException, ClassNotFoundException {
        // given
        IndexIterator[] indexIterators = new IndexIterator[] {
                mock(IndexIterator.class),
                mock(IndexIterator.class)
        };
        when(indexIterators[0].nextDocument())
                .thenReturn(1L)
                .thenReturn(2L)
                .thenReturn(END_OF_LIST);
        when(indexIterators[0].count())
                .thenReturn(2)
                .thenReturn(4);
        when(indexIterators[0].frequency()).thenReturn(6L);
        when(indexIterators[1].nextDocument())
                .thenReturn(1L)
                .thenReturn(2L)
                .thenReturn(END_OF_LIST);
        when(indexIterators[1].count())
                .thenReturn(2)
                .thenReturn(4);
        when(indexIterators[1].frequency()).thenReturn(6L);

        StatisticalShardRepresentation representation = spy(new StatisticalShardRepresentation("dummy"));
        doReturn(10L).when(representation).occurrency(eq(indexIterators[0]));
        doReturn(100L).when(representation).collectionSize(eq(indexIterators[0]));
        doReturn(10L).when(representation).documentSize(eq(indexIterators[0]));
        doReturn(10L).when(representation).occurrency(eq(indexIterators[1]));
        doReturn(100L).when(representation).collectionSize(eq(indexIterators[1]));
        doReturn(10L).when(representation).documentSize(eq(indexIterators[1]));
        representation.mu = 10.0;

        // when
        TermStats actual = representation.termStats(indexIterators);

        // then
        assertEquals(actual.expectedValue, -0.547235724334295, 0.000001);
        assertEquals(actual.variance, 0.620679110800021, 0.000001);
        assertEquals(actual.minValue, -1.897119984885881, 0.000001);
    }

    @Test
    public void calc() throws IllegalAccessException, ConfigurationException, IOException, InstantiationException, ClassNotFoundException, URISyntaxException, NoSuchMethodException, InvocationTargetException {
        // given
        File clusterDir = getFileFromResourcePath("clusters");
        StatisticalShardRepresentation representation = new StatisticalShardRepresentation(clusterDir.getAbsolutePath() + "/gov2C-0");

        // when
        TermIterator it = representation.calc();

        // then
        int counter = 0;
        while (it.hasNext()) {
            counter++;
            TermStats t = it.next();
            assertThat(t.expectedValue, lessThanOrEqualTo(0.0));
            assertThat(t.minValue, lessThanOrEqualTo(0.0));
            assertThat(t.variance, greaterThanOrEqualTo(0.0));
        }
        assertEquals(4194, counter);
    }

    @Test
    public void calcCluster() throws IllegalAccessException, ConfigurationException, IOException, InstantiationException, ClassNotFoundException, URISyntaxException, NoSuchMethodException, InvocationTargetException {
        // given
        File clusterDir = getFileFromResourcePath("clusters");
        StatisticalShardRepresentation representation = new StatisticalShardRepresentation(clusterDir.getAbsolutePath() + "/gov2C");

        // when
        TermIterator it = representation.calc();

        // then
        int counter = 0;
        while (it.hasNext()) {
            counter++;
            TermStats t = it.next();
            assertThat(t.expectedValue, lessThanOrEqualTo(0.0));
            assertThat(t.minValue, lessThanOrEqualTo(0.0));
            assertThat(t.variance, greaterThanOrEqualTo(0.0));
        }
        assertEquals(16741, counter);
    }

    @Test
    public void calcSkip() throws IllegalAccessException, ConfigurationException, IOException, InstantiationException, ClassNotFoundException, URISyntaxException, NoSuchMethodException, InvocationTargetException {
        // given
        File clusterDir = getFileFromResourcePath("clusters");
        StatisticalShardRepresentation representation = new StatisticalShardRepresentation(clusterDir.getAbsolutePath() + "/gov2C-0");

        // when
        TermIterator it = representation.calc();

        // then
        TermStats t = it.skip(4193);
        assertThat(t.expectedValue, not(equalTo(0.0)));
        assertThat(t.minValue, not(equalTo(0.0)));
        assertThat(it.hasNext(), equalTo(Boolean.FALSE));
    }

    @Test
    public void termIterator() throws IllegalAccessException, URISyntaxException, IOException, InstantiationException, NoSuchMethodException, ConfigurationException, InvocationTargetException, ClassNotFoundException {
        // given
        File clusterDir = getFileFromResourcePath("clusters");
        StatisticalShardRepresentation representation = new StatisticalShardRepresentation(clusterDir.getAbsolutePath() + "/gov2C-0");

        // when
        TermIterator it = representation.termIterator();

        // then
        int counter = 0;
        while (it.hasNext()) {
            counter++;
            TermStats t = it.next();
            assertThat(t.expectedValue, not(equalTo(0.0)));
            assertThat(t.minValue, not(equalTo(0.0)));
        }
        assertEquals(counter, 4194);
    }

    @Test
    public void termIteratorSkip() throws IllegalAccessException, ConfigurationException, IOException, InstantiationException, ClassNotFoundException, URISyntaxException, NoSuchMethodException, InvocationTargetException {
        // given
        File clusterDir = getFileFromResourcePath("clusters");
        StatisticalShardRepresentation representation = new StatisticalShardRepresentation(clusterDir.getAbsolutePath() + "/gov2C-0");

        // when
        TermIterator it = representation.termIterator();

        // then
        TermStats t = it.skip(4194);
        assertThat(t.expectedValue, not(equalTo(0.0)));
        assertThat(t.minValue, not(equalTo(0.0)));
        assertThat(it.hasNext(), equalTo(Boolean.FALSE));
    }

    @Test
    public void queryStats() throws IllegalAccessException, ConfigurationException, IOException, InstantiationException, ClassNotFoundException, URISyntaxException, NoSuchMethodException, InvocationTargetException {
        // given
        String basename = buildIndexA();
        StatisticalShardRepresentation representation = new StatisticalShardRepresentation(basename);
        TermIterator it = representation.calc(1.0);
        representation.write(it);

        // when
        TermStats t = representation.queryStats(new long[] { 0, 1 });

        // then
        assertEquals(-2.8089009797822153, t.expectedValue, 0.000001);
        assertEquals(0.2243828408711066, t.variance, 0.000001);
        assertEquals(-3.282591639254308, t.minValue, 0.000001);
    }

}
