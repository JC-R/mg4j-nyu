package edu.nyu.tandon.shard.ranking.taily;

import edu.nyu.tandon.test.BaseTest;
import junit.framework.TestCase;
import org.junit.Before;
import org.junit.Test;

import static junit.framework.TestCase.assertEquals;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsNot.not;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class StatisticalShardRepresentationClusterTest extends BaseTest {

    public StatisticalShardRepresentation representation;

    @Before
    public void init() throws Exception {
        String basename = buildCluster();
        representation = new StatisticalShardRepresentation(basename);
    }

    @Test
    public void calc() throws Exception {

        // when
        StatisticalShardRepresentation.TermIterator it = representation.calc(1.0);
        representation.write(it);
        it = representation.termIterator();

        // then
        // a
        // 0: -0.6411874416292341
        // 1: -1.6433394641097818
        StatisticalShardRepresentation.TermStats t = it.next();
        TestCase.assertEquals(-1.6433394641097818, t.minValue, 0.000001);
        TestCase.assertEquals(-1.142263452869508, t.expectedValue, 0.000001);
        TestCase.assertEquals(0.2510771690404632, t.variance, 0.000001);
        // b
        t = it.next();
        // f
        t = it.next();
        // g
        // 0: -1.6433394641097818
        // 1: -1.6433394641097818
        // 2: -1.0216512475319812
        t = it.next();
        TestCase.assertEquals(-1.6433394641097818, t.minValue, 0.000001);
        TestCase.assertEquals(-1.4361100585838482, t.expectedValue, 0.000001);
        TestCase.assertEquals(0.08588805302926383, t.variance, 0.000001);

    }

    @Test
    public void queryStats() throws Exception {
        // given
        String basename = buildCluster();
        StatisticalShardRepresentation representation = new StatisticalShardRepresentation(basename);
        StatisticalShardRepresentation.TermIterator it = representation.calc(1.0);
        representation.write(it);

        // when
        StatisticalShardRepresentation.TermStats t = representation.queryStats(new long[] { 0, 3 });

        // then
        assertEquals(-2.578373511453356, t.expectedValue, 0.000001);
        assertEquals(0.336965222069727, t.variance, 0.000001);
        assertEquals(-3.286678928219564, t.minValue, 0.000001);
    }

    @Test
    public void termIteratorSkip() throws Exception {
        // given
        String basename = buildCluster();
        StatisticalShardRepresentation representation = new StatisticalShardRepresentation(basename);
        StatisticalShardRepresentation.TermIterator it = representation.calc(1.0);
        representation.write(it);

        // when
        it = representation.termIterator();

        // then
        StatisticalShardRepresentation.TermStats t = it.next(0);
        TestCase.assertEquals(-1.6433394641097818, t.minValue, 0.000001);
        TestCase.assertEquals(-1.142263452869508, t.expectedValue, 0.000001);
        TestCase.assertEquals(0.2510771690404632, t.variance, 0.000001);
        t = it.next(3);
        TestCase.assertEquals(-1.6433394641097818, t.minValue, 0.000001);
        TestCase.assertEquals(-1.4361100585838482, t.expectedValue, 0.000001);
        TestCase.assertEquals(0.08588805302926383, t.variance, 0.000001);
    }

}
