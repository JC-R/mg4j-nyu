package edu.nyu.tandon.shard.ranking.taily;

import edu.nyu.tandon.shard.ranking.taily.StatisticalShardRepresentation.Term;
import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.index.IndexReader;
import it.unimi.dsi.big.util.StringMap;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.math3.special.Gamma;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.function.Function;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class TailyShardEvaluator {

    protected StatisticalShardRepresentation statisticalRepresentation;
    protected Index index;
    protected StringMap<? extends CharSequence> termMap;

    protected static double epsilon = 0.000001;

    public TailyShardEvaluator(Index index, StatisticalShardRepresentation statisticalRepresentation,
                               StringMap<? extends CharSequence> termMap)
            throws IllegalAccessException, InvocationTargetException, NoSuchMethodException, IOException, InstantiationException, URISyntaxException, ConfigurationException, ClassNotFoundException {

        this.index = index;
        this.termMap = termMap;
        this.statisticalRepresentation = statisticalRepresentation;
    }

    public TailyShardEvaluator(Index index, StatisticalShardRepresentation statisticalRepresentation)
            throws IllegalAccessException, InvocationTargetException, NoSuchMethodException, IOException, InstantiationException, URISyntaxException, ConfigurationException, ClassNotFoundException {

        this(index, statisticalRepresentation, index.termMap);
    }

    public Function<Double, Double> cdf(long[] terms) {
        try {
            Term t = statisticalRepresentation.queryScore(terms);
            return cdf(t.expectedValue, t.variance);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected Function<Double, Double> cdf(double expectedValue, double variance) {
        double k = expectedValue * expectedValue / variance;
        double theta = variance / expectedValue;
        return (s) -> {
            try {
                return Gamma.regularizedGammaQ(k, s / theta);
            } catch (Exception e) {
                throw new RuntimeException(String.format("Failed to compute regularizedGammaQ(%f, %f)",
                        k, s / theta), e);
            }
        };
    }

    protected static double invRegularizedGammaQ(double a, double y) {
        double low = 0;
        double high = Double.MAX_VALUE;
        double inv;
        double mid;
        do {
            mid = low + (high - low) / 2;
            inv = Gamma.regularizedGammaQ(a, mid);
            if (inv > y) low = mid;
            else high = mid;
        } while (Math.abs(inv - y) > epsilon && high - low > epsilon);
        return mid;
    }

    public Function<Double, Double> icdf(double expectedValue, double variance) {
        double k = expectedValue * expectedValue / variance;
        double theta = variance / expectedValue;
        return (p) -> invRegularizedGammaQ(k, p) * theta;
    }

    public long[] frequencies(List<String> terms) throws IOException {
        return frequencies(termIds(terms));
    }

    protected long[] frequencies(long[] terms) throws IOException {
        try (IndexReader indexReader = index.getReader()) {
            long[] f = new long[terms.length];
            for (int i = 0; i < terms.length; i++) {
                f[i] = terms[i] >= 0 ? indexReader.documents(terms[i]).frequency() : 0;
            }
            return f;
        }
    }

    protected double any(long[] frequencies) throws IOException {
        return any(frequencies, index.numberOfDocuments);
    }

    public static double any(long[] frequencies, double D) throws IOException {
        double product = 1;
        for (long frequency : frequencies) product *= 1.0 - frequency / D;
        return D * (1.0 - product);
    }

    protected double all(long[] frequencies) throws IOException {
        return all(frequencies, index.numberOfDocuments);
    }

    public static double all(long[] frequencies, double D) throws IOException {
        double any = any(frequencies, D);
        double product = 1.0;
        for (long frequency : frequencies) product *= frequency / any;
        return any * product;
    }

    public double all(List<String> terms) throws IOException {
        return all(frequencies(terms));
    }

    protected long[] termIds(List<String> terms) {
        long[] ids = new long[terms.size()];
        int i = 0;
        for (String term : terms) ids[i++] = termMap.getLong(term);
        return ids;
    }

    public double estimateDocsAboveCutoff(List<String> terms, double scoreCutoff) throws IOException {
        long[] termIds = termIds(terms);
        long[] frequencies = frequencies(termIds);
        return all(frequencies) * cdf(termIds).apply(scoreCutoff);
    }

}
