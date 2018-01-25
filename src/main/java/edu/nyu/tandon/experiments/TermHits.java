package edu.nyu.tandon.experiments;

import com.martiansoftware.jsap.*;
import edu.nyu.tandon.query.Query;
import edu.nyu.tandon.utils.Utils;
import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.index.IndexIterator;
import it.unimi.di.big.mg4j.index.IndexReader;
import it.unimi.di.big.mg4j.index.TermProcessor;
import it.unimi.di.big.mg4j.search.score.BM25Scorer;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static it.unimi.di.big.mg4j.search.DocumentIterator.END_OF_LIST;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class TermHits {

    private static class Acc implements Comparable<Acc> {
        public int doc;
        public double score;
        public int term_hits;
        public Acc(int doc, double score, int term_hits) {
            this.doc = doc;
            this.score = score;
            this.term_hits = term_hits;
        }

        @Override
        public int compareTo(Acc rhs) {
            return Double.compare(-this.score, -rhs.score);
        }
    }

    public static final Logger LOGGER = LoggerFactory.getLogger(TermHits.class);

    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws Exception {

        SimpleJSAP jsap = new SimpleJSAP(Query.class.getName(), ".",
                new Parameter[]{
                        new UnflaggedOption("basename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of the index."),
                        new UnflaggedOption("input", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The query input file.")
                });

        final JSAPResult jsapResult = jsap.parse(args);
        if (jsap.messagePrinted()) return;

        String basename = jsapResult.getString("basename");

        Index index = Index.getInstance(basename, true, true, true);
        TermProcessor termProcessor = index.termProcessor;

        int queryId = 0;
        String queryFile = jsapResult.getString("input");
        System.out.println("query\trank\tdoc\tquery_terms\tterm_hits");
        try (FileInputStream queryStream = new FileInputStream(queryFile);
             IndexReader indexReader = index.getReader()) {
            LineIterator queries = IOUtils.lineIterator(queryStream, StandardCharsets.UTF_8);
            while (queries.hasNext()) {
                String query = queries.nextLine();
                List<String> terms = Utils.extractTerms(query, termProcessor);
                double[] scores = new double[(int) index.numberOfDocuments];
                int[] counts = new int[(int) index.numberOfDocuments];
                for (String term : terms) {
                    IndexIterator indexIterator = indexReader.documents(term);
                    BM25Scorer scorer = new BM25Scorer();
                    scorer.wrap(indexIterator);
                    long doc;
                    while ((doc = indexIterator.nextDocument()) != END_OF_LIST) {
                        scores[(int)doc] += scorer.score();
                        counts[(int)doc] += 1;
                    }
                }
                List<Acc> accs = new ArrayList<>();
                for (int idx = 0; idx < scores.length; ++idx) {
                    if (scores[idx] > 0.0) {
                        accs.add(new Acc(idx, scores[idx], counts[idx]));
                    }
                }
                Collections.sort(accs);

                for (int rank = 0; rank < Math.min(30, accs.size()); ++rank) {
                    System.out.println(String.format("%d\t%d\t%d\t%d\t%d",
                            queryId,
                            rank,
                            accs.get(rank).doc,
                            terms.size(),
                            accs.get(rank).term_hits));
                }
                ++queryId;
            }
        }
    }

}
