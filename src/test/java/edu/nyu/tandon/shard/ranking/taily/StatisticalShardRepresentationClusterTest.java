package edu.nyu.tandon.shard.ranking.taily;

import edu.nyu.tandon.test.BaseTest;
import junit.framework.TestCase;
import org.junit.Before;
import org.junit.Test;

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

}
