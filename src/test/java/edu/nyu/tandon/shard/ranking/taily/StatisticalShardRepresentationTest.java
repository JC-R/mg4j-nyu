package edu.nyu.tandon.shard.ranking.taily;

import edu.nyu.tandon.shard.ranking.taily.StatisticalShardRepresentation.Term;
import edu.nyu.tandon.shard.ranking.taily.StatisticalShardRepresentation.TermIterator;
import edu.nyu.tandon.test.BaseTest;
import it.unimi.di.big.mg4j.index.IndexIterator;
import org.apache.commons.configuration.ConfigurationException;
import org.hamcrest.core.IsNot;
import org.hamcrest.core.IsNot.*;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;

import static it.unimi.di.big.mg4j.search.DocumentIterator.END_OF_LIST;
import static junit.framework.TestCase.assertEquals;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.core.IsNot.not;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class StatisticalShardRepresentationTest extends BaseTest {

    @Test
    public void termStats() throws IOException {
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
        Term actual = representation.termStats(indexIterator);

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
            Term t = it.next();
            assertThat(t.expectedValue, not(equalTo(0.0)));
            assertThat(t.minValue, not(equalTo(0.0)));
        }
        assertEquals(counter, 4194);
    }

    @Test
    public void calcSkip() throws IllegalAccessException, ConfigurationException, IOException, InstantiationException, ClassNotFoundException, URISyntaxException, NoSuchMethodException, InvocationTargetException {
        // given
        File clusterDir = getFileFromResourcePath("clusters");
        StatisticalShardRepresentation representation = new StatisticalShardRepresentation(clusterDir.getAbsolutePath() + "/gov2C-0");

        // when
        TermIterator it = representation.calc();

        // then
        Term t = it.skip(4193);
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
            Term t = it.next();
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
        Term t = it.skip(4193);
        assertThat(t.expectedValue, not(equalTo(0.0)));
        assertThat(t.minValue, not(equalTo(0.0)));
        assertThat(it.hasNext(), equalTo(Boolean.FALSE));
    }

}
