package edu.nyu.tandon.experiments;

import com.martiansoftware.jsap.*;
import edu.nyu.tandon.query.Query;
import edu.nyu.tandon.utils.Utils;
import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.index.IndexIterator;
import it.unimi.di.big.mg4j.index.IndexReader;
import it.unimi.di.big.mg4j.index.snowball.EnglishStemmer;
import it.unimi.di.big.mg4j.search.score.BM25Scorer;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

import static it.unimi.di.big.mg4j.search.DocumentIterator.END_OF_LIST;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class ExtractPostingListFeatures {

    public static final Logger LOGGER = LoggerFactory.getLogger(ExtractPostingListFeatures.class);

    public static void processTerm(IndexIterator indexIterator, String term) throws IOException {
        indexIterator.term(term);
        double maxScore = 0.0;
        BM25Scorer scorer = new BM25Scorer();
        scorer.wrap(indexIterator);
        while (indexIterator.nextDocument() != END_OF_LIST) {
            double score = scorer.score();
            maxScore = Math.max(score, maxScore);
        }
        System.out.println(String.format("%d,%d,%f",
                indexIterator.termNumber(),
                indexIterator.frequency(),
                maxScore));
    }

    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws Exception {

        SimpleJSAP jsap = new SimpleJSAP(Query.class.getName(), ".",
                new Parameter[]{
                        new FlaggedOption("input", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'i', "input", "Input queries (limit extracting to those terms)."),
                        new UnflaggedOption("basename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of the index.")
                });

        final JSAPResult jsapResult = jsap.parse(args);
        if (jsap.messagePrinted()) return;

        String basename = jsapResult.getString("basename");
        String termFilePath = String.format("%s.terms", basename);

        Index index = Index.getInstance(basename, true, true, true);

        System.out.println("termid,length,maxscore");

        if (jsapResult.userSpecified("input")) {
            try (IndexReader indexReader = index.getReader()) {
                List<String> terms = Files.readAllLines(Paths.get(jsapResult.getString("input")));
                String[] distinctTerms = terms.stream()
                        .flatMap((s) -> Utils.extractTerms(s, new EnglishStemmer()).stream())
                        .distinct()
                        .sorted()
                        .toArray(String[]::new);
                for (String term : distinctTerms) {
                    IndexIterator indexIterator = indexReader.documents(term);
                    processTerm(indexIterator, term);
                }
            }
        }
        else {
            try (FileInputStream inputStream = new FileInputStream(termFilePath);
                 IndexReader indexReader = index.getReader()) {
                LineIterator terms = IOUtils.lineIterator(inputStream, StandardCharsets.UTF_8);
                IndexIterator indexIterator;
                while ((indexIterator = indexReader.nextIterator()) != null) {
                    String term = terms.nextLine();
                    processTerm(indexIterator, term);
                }
            }
        }
    }

}
