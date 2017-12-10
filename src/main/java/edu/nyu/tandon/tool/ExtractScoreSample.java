package edu.nyu.tandon.tool;

import com.martiansoftware.jsap.*;
import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.index.IndexIterator;
import it.unimi.di.big.mg4j.index.IndexReader;
import it.unimi.di.big.mg4j.search.score.BM25Scorer;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;

import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Random;

import static it.unimi.di.big.mg4j.search.DocumentIterator.END_OF_LIST;

public class ExtractScoreSample {

    public static double maxScore(IndexIterator indexIterator) throws IOException {
        double maxScore = 0.0;
        BM25Scorer scorer = new BM25Scorer();
        scorer.wrap(indexIterator);
        while (indexIterator.nextDocument() != END_OF_LIST) {
            double score = scorer.score();
            maxScore = Math.max(score, maxScore);
        }
        return maxScore;
    }

    public static void main(String[] args) throws JSAPException, IllegalAccessException, InvocationTargetException, NoSuchMethodException, IOException, InstantiationException, URISyntaxException, ConfigurationException, ClassNotFoundException {

        SimpleJSAP jsap = new SimpleJSAP(ExtractScoreSample.class.getName(), "",
                new Parameter[]{
                        new UnflaggedOption("basename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of the index."),
                        new FlaggedOption("sampleSize", JSAP.INTEGER_PARSER, "1000000", JSAP.REQUIRED, 's', "sample-size", "The approximated number of terms to sample."),
                });

        JSAPResult jsapResult = jsap.parse(args);
        if (jsap.messagePrinted()) return;

        String basename = jsapResult.getString("basename");
        String termFilePath = String.format("%s.terms", basename);
        Index index = Index.getInstance(basename, true, true, true);

        long sampleSize = jsapResult.getLong("sampleSize");

        System.out.println("term,score");

        Random random = new Random(System.currentTimeMillis());

        try (FileInputStream inputStream = new FileInputStream(termFilePath);
             IndexReader reader = index.getReader()) {
            LineIterator terms = IOUtils.lineIterator(inputStream, StandardCharsets.UTF_8);
            IndexIterator iterator = null;
            while ((iterator = reader.nextIterator()) != null) {
                long doc;
                String term = terms.nextLine();
                if ((random.nextLong() % index.numberOfTerms) < sampleSize) {
                    continue;
                }
                iterator.term(term);
                BM25Scorer scorer = new BM25Scorer();
                scorer.wrap(iterator);
                while (iterator.nextDocument() != END_OF_LIST) {
                    double score = scorer.score();
                }
            }
        }
    }
}
