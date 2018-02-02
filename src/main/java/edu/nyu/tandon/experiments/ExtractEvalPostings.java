package edu.nyu.tandon.experiments;

import com.martiansoftware.jsap.*;
import edu.nyu.tandon.query.Query;
import edu.nyu.tandon.utils.Utils;
import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.index.IndexIterator;
import it.unimi.di.big.mg4j.index.IndexReader;
import it.unimi.di.big.mg4j.index.TermProcessor;
import it.unimi.di.big.mg4j.search.score.BM25Scorer;
import it.unimi.dsi.lang.MutableString;

import java.io.FileWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static it.unimi.di.big.mg4j.search.DocumentIterator.END_OF_LIST;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class ExtractEvalPostings {

    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws Exception {

        SimpleJSAP jsap = new SimpleJSAP(Query.class.getName(), ".",
                new Parameter[]{
                        new FlaggedOption("input", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'i', "input", "Input queries (limit extracting to those terms)."),
                        new UnflaggedOption("basename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of the index."),
                        new UnflaggedOption("output-base", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of output files (-queries.csv and -postings.csv will appended)")
                });

        final JSAPResult jsapResult = jsap.parse(args);
        if (jsap.messagePrinted()) return;

        String basename = jsapResult.getString("basename");
        String queryFile = jsapResult.getString("input");
        String queriesOutFile = jsapResult.getString("output-base") + "-queries.csv";
        String postingsOutFile = jsapResult.getString("output-base") + "-postings.csv";

        Index index = Index.getInstance(basename, true, true, true);
        TermProcessor termProcessor = index.termProcessor;

        try (FileWriter queriesOut = new FileWriter(queriesOutFile);
             FileWriter postingsOut = new FileWriter(postingsOutFile);
             IndexReader indexReader = index.getReader()) {

            queriesOut.append("query,term,stemmed,termid\n");
            postingsOut.append("termid,docid,score\n");

            Map<String, Integer> seen = new HashMap<>();

            int queryId = 0;
            int termCount = 0;
            List<String> queries = Files.readAllLines(Paths.get(queryFile));
            for (String query : queries) {
                List<String> terms = Utils.extractTerms(query, null);
                for (String term : terms) {
                    Integer termId = seen.get(term);
                    MutableString m = new MutableString(term);
                    termProcessor.processTerm(m);
                    String stemmed = m.toString();
                    if (termId == null) {
                        termId = termCount++;
                        seen.put(term, termId);
                        IndexIterator indexIterator = indexReader.documents(stemmed);
                        BM25Scorer scorer = new BM25Scorer();
                        scorer.wrap(indexIterator);
                        while (indexIterator.nextDocument() != END_OF_LIST) {
                            long docId = indexIterator.document();
                            double score = scorer.score();
                            postingsOut.append(String.format("%d,%d,%f\n",
                                    termId, docId, score));
                        }
                    }
                    queriesOut.append(String.format("%d,%s,%s,%d\n",
                            queryId++, term, stemmed, termId));
                }
            }
        }
    }

}
