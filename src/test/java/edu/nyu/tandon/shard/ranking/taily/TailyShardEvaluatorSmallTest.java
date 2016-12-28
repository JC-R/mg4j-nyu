package edu.nyu.tandon.shard.ranking.taily;

import edu.nyu.tandon.test.BaseTest;
import it.unimi.di.big.mg4j.index.Index;
import junit.framework.TestCase;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.math3.special.Gamma;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.function.Function;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertEquals;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class TailyShardEvaluatorSmallTest extends BaseTest {

    TailyShardEvaluator evaluator;

    @Before
    public void init() throws IllegalAccessException, ConfigurationException, IOException, InstantiationException, ClassNotFoundException, URISyntaxException, NoSuchMethodException, InvocationTargetException {
        String basename = buildIndexA();
        StatisticalShardRepresentation representation = new StatisticalShardRepresentation(basename);
        representation.write(representation.calc(1.0));
        evaluator = new TailyShardEvaluator(Index.getInstance(basename), representation);
    }

    @Test
    public void frequencies() throws IllegalAccessException, ConfigurationException, IOException, InstantiationException, ClassNotFoundException, URISyntaxException, NoSuchMethodException, InvocationTargetException {
        // when
        long[] actual = evaluator.frequencies(Arrays.asList("a", "b", "f", "g", "h", "x", "z", "y"));

        // then
        assertThat(actual[0], equalTo(2L));
        assertThat(actual[1], equalTo(1L));
        assertThat(actual[2], equalTo(2L));
        assertThat(actual[3], equalTo(2L));
        assertThat(actual[4], equalTo(1L));
        assertThat(actual[5], equalTo(1L));
        assertThat(actual[6], equalTo(1L));
        assertThat(actual[7], equalTo(2L));
    }

    @Test
    public void all() throws IllegalAccessException, ConfigurationException, IOException, InstantiationException, ClassNotFoundException, URISyntaxException, NoSuchMethodException, InvocationTargetException {
        assertEquals(evaluator.all(Arrays.asList("a", "b")), 6.0 / 7.0, 0.1);
    }

    @Test
    public void cdf() {
        // exp = 2.1910990202177847
        // var = 0.2243828408711066
        // k = 21.396087587451262
        // theta = 0.10240652695322004

        // when
        Function<Double, Double> actual = evaluator.cdf(evaluator.termIds(Arrays.asList("a", "b")), -5);
        Function<Double, Double> expected = (s) -> Gamma.regularizedGammaQ(21.396087587451262, s / 0.10240652695322004);

        // then
        assertEquals(1.0, actual.apply(0.0), 0.00001);
        assertEquals(0.0, actual.apply(5.0), 0.00001);
        double X[] = new double[] { 0.25, 0.5, 0.75, 1, 2,5, 5 };
        for (double x : X) {
            assertEquals(expected.apply(x), actual.apply(x), 0.00001);
        }
    }

    @Test
    public void invRegularizedGammaQ() {
        double X[] = new double[] { 0.25, 0.5, 0.75, 1, 2.5, 5 };
        for (double x : X) {
            double y = Gamma.regularizedGammaQ(1, x);
            assertEquals(x, TailyShardEvaluator.invRegularizedGammaQ(1, y), 0.00001);
        }
    }

    @Test
    public void invRegularizedGammaQ25() {
        double X[] = new double[] { 10, 25, 50 };
        for (double x : X) {
            double y = Gamma.regularizedGammaQ(25, x);
            assertEquals(x, TailyShardEvaluator.invRegularizedGammaQ(25, y), 0.00001);
        }
    }

    @Test
    public void icdf() {
        // when
        Function<Double, Double> f = TailyShardEvaluator.cdf(5, 1);
        Function<Double, Double> inv = TailyShardEvaluator.icdf(5, 1);

        // then
        double X[] = new double[] { 2, 5, 10 };
        for (double x : X) {
            double y = f.apply(x);
            assertEquals(x, inv.apply(y), 0.00001);
        }
    }

    @Test
    public void estimateDocsAboveCutoff() throws IllegalAccessException, ConfigurationException, IOException, InstantiationException, ClassNotFoundException, URISyntaxException, NoSuchMethodException, InvocationTargetException {
        // cdf = 0.24349576427715708
        // all = 6/7
        assertEquals(0.20871065509470604, evaluator.estimateDocsAboveCutoff(Arrays.asList("a", "b"), 2.5, -5), 0.00001);
    }

    @Test
    public void calc() throws IllegalAccessException, ConfigurationException, IOException, InstantiationException, ClassNotFoundException, URISyntaxException, NoSuchMethodException, InvocationTargetException {
        // given
        String basename = buildIndexA();
        StatisticalShardRepresentation representation = new StatisticalShardRepresentation(basename);

        // when
        StatisticalShardRepresentation.TermIterator it = representation.calc(1.0);

        // then
        /**
         * Inverted Index:
         * a | 0[3] 1[1]
         * b | 0[1]
         * f | 1[1] 2[1]
         * g | 0[1] 2[1]
         * h | 2[2]
         * x | 1[1]
         * y | 1[1] 2[1]
         * z | 1[1]
         *
         * f_a(0) = -0.6079893722196386
         * f_a(1) = -1.5553706911638245
         */
        StatisticalShardRepresentation.TermStats t = it.next();
        TestCase.assertEquals(-1.5553706911638245, t.minValue, 0.000001);
        TestCase.assertEquals(-1.0816800316917314, t.expectedValue, 0.000001);
        TestCase.assertEquals(0.2243828408711066, t.variance, 0.000001);

        t = it.next();
        TestCase.assertEquals(-1.7272209480904839, t.minValue, 0.000001);
        TestCase.assertEquals(-1.7272209480904839, t.expectedValue, 0.000001);
        TestCase.assertEquals(0.0, t.variance, 0.000001);
    }

    @Test
    public void write() throws IllegalAccessException, ConfigurationException, IOException, InstantiationException, ClassNotFoundException, URISyntaxException, NoSuchMethodException, InvocationTargetException {
        // given
        String basename = buildIndexA();
        StatisticalShardRepresentation representation = new StatisticalShardRepresentation(basename);

        // when
        StatisticalShardRepresentation.TermIterator it = representation.calc(1.0);
        representation.write(it);

        // then
        StatisticalShardRepresentation.TermIterator stored = representation.calc(1.0);
        StatisticalShardRepresentation.TermIterator calculated = representation.termIterator();
        while (stored.hasNext() && calculated.hasNext()) {
            StatisticalShardRepresentation.TermStats s = stored.next();
            StatisticalShardRepresentation.TermStats c = calculated.next();
            assertEquals(s.expectedValue, c.expectedValue, 0.1);
            assertEquals(s.minValue, c.minValue, 0.1);
            assertEquals(s.variance, c.variance, 0.1);
            assertThat(s.expectedValue, lessThanOrEqualTo(0.0));
            assertThat(s.minValue, lessThanOrEqualTo(0.0));
            assertThat(s.variance, greaterThanOrEqualTo(0.0));
        }
        assertThat(stored.hasNext(), equalTo(false));
        assertThat(calculated.hasNext(), equalTo(false));
    }

}
