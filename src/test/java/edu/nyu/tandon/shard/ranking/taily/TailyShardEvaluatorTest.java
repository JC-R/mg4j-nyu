package edu.nyu.tandon.shard.ranking.taily;

import edu.nyu.tandon.test.BaseTest;
import it.unimi.di.big.mg4j.index.Index;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.math.MathException;
import org.apache.commons.math.special.Gamma;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class TailyShardEvaluatorTest extends BaseTest {

    private static String basename0;

    @BeforeClass
    public static void init() {
        File clusterDir = getFileFromResourcePath("clusters");
        basename0 = clusterDir.getAbsolutePath() + "/gov2C-0";
    }

    @Test
    public void termIds() throws IllegalAccessException, InvocationTargetException, NoSuchMethodException, IOException, InstantiationException, URISyntaxException, ConfigurationException, ClassNotFoundException {
        // given
        StatisticalShardRepresentation representation = new StatisticalShardRepresentation(basename0);
        TailyShardEvaluator evaluator = new TailyShardEvaluator(Index.getInstance(basename0, true, true), representation);

        // when
        long[] ids = evaluator.termIds(Arrays.asList("0", "gain", "русски"));

        // then
        assertThat(ids, equalTo(new long[] { 0, 1761, 4193 }));
    }

    @Test
    public void anyWithD() throws IOException {
        assertThat(TailyShardEvaluator.any(new long[] {10, 5, 2}, 100.0),
                equalTo(16.21));
    }

    @Test
    public void any() throws IOException, IllegalAccessException, URISyntaxException, InstantiationException, ConfigurationException, NoSuchMethodException, InvocationTargetException, ClassNotFoundException {
        Index index = Index.getInstance(basename0, true, true);
        TailyShardEvaluator evaluator = new TailyShardEvaluator(index, mock(StatisticalShardRepresentation.class));
        assertThat(evaluator.any(new long[] {10, 5, 2}),
                equalTo(16.21));
    }

    @Test
    public void allWithD() throws IOException {
        assertEquals(TailyShardEvaluator.all(new long[] {10, 5, 2}, 100.0), 0.38056949179891775, 0.000001);
    }

    @Test
    public void all() throws IOException, IllegalAccessException, URISyntaxException, InstantiationException, ConfigurationException, NoSuchMethodException, InvocationTargetException, ClassNotFoundException {
        Index index = Index.getInstance(basename0, true, true);
        TailyShardEvaluator evaluator = new TailyShardEvaluator(index, mock(StatisticalShardRepresentation.class));
        assertEquals(evaluator.all(new long[] {10, 5, 2}), 0.38056949179891775, 0.000001);
    }

    @Test
    public void allWithTerms() throws IOException, IllegalAccessException, URISyntaxException, InstantiationException, ConfigurationException, NoSuchMethodException, InvocationTargetException, ClassNotFoundException {
        Index index = Index.getInstance(basename0, true, true);
        TailyShardEvaluator evaluator = spy(new TailyShardEvaluator(index, mock(StatisticalShardRepresentation.class)));
        List<String> terms = Arrays.asList("a", "b", "c");
        doReturn(new long[] {1, 2, 3}).when(evaluator).termIds(terms);
        doReturn(new long[] {10, 5, 2}).when(evaluator).frequencies(eq(new long[] {1, 2, 3}));
        assertEquals(evaluator.all(terms), 0.38056949179891775, 0.000001);
    }

    @Test
    public void cdf() throws IllegalAccessException, URISyntaxException, IOException, InstantiationException, NoSuchMethodException, ConfigurationException, InvocationTargetException, ClassNotFoundException {
        // given
        TailyShardEvaluator evaluator = new TailyShardEvaluator(mock(Index.class), mock(StatisticalShardRepresentation.class));
        // when
        Function<Double, Double> cdf = evaluator.cdf(5, 1);
        // then
        assertEquals(cdf.apply(1.0), 0.9999999998400414, 0.000001);
        assertEquals(cdf.apply(2.0), 0.9999530506185732, 0.000001);
        assertEquals(cdf.apply(3.0), 0.9888352197284497, 0.000001);
    }

    @Test
    public void icdf() throws IllegalAccessException, URISyntaxException, IOException, InstantiationException, NoSuchMethodException, ConfigurationException, InvocationTargetException, ClassNotFoundException {
        // given
        TailyShardEvaluator evaluator = new TailyShardEvaluator(mock(Index.class), mock(StatisticalShardRepresentation.class));
        // when
        Function<Double, Double> cdf = evaluator.cdf(5, 1);
        Function<Double, Double> icdf = evaluator.icdf(5, 1);
        // then
        assertEquals(2.0, icdf.apply(cdf.apply(2.0)), 0.000001);
        assertEquals(3.0, icdf.apply(cdf.apply(3.0)), 0.000001);
        assertEquals(4.0, icdf.apply(cdf.apply(4.0)), 0.000001);
    }

    @Test
    public void invRegularizedGammaQ() throws IllegalAccessException, URISyntaxException, IOException, InstantiationException, NoSuchMethodException, ConfigurationException, InvocationTargetException, ClassNotFoundException, MathException {

        assertEquals(0.0, TailyShardEvaluator.invRegularizedGammaQ(1.0, 1.0), 0.000001);

        double x = TailyShardEvaluator.invRegularizedGammaQ(1.0, 0.0);
        assertEquals(0.0, Gamma.regularizedGammaQ(1.0, x), 0.000001);

        x = TailyShardEvaluator.invRegularizedGammaQ(1.0, 0.5);
        assertEquals(0.5, Gamma.regularizedGammaQ(1.0, x), 0.000001);

        x = TailyShardEvaluator.invRegularizedGammaQ(2.0, 0.75);
        assertEquals(0.75, Gamma.regularizedGammaQ(2.0, x), 0.000001);
    }

}
