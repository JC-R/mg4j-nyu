package edu.nyu.tandon.utils;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class Stats {

    private double sum = 0;
    private double sumOfsquares = 0;
    private double product = 1;
    private double invSum = 0;
    private long count = 0;

    public Stats add(double value) {
        sum += value;
        sumOfsquares += value * value;
        product *= value;
        invSum += 1.0 / value;
        count++;
        return this;
    }

    public double aMean() {
        return sum / count;
    }

    public double gMean() {
        return Math.pow(product, 1.0 / count);
    }

    public double hMean() {
        return (double) count / invSum;
    }

    public double variance() {
        double aMean = aMean();
        return (sumOfsquares / count) - aMean * aMean;
    }

}
