package edu.nyu.tandon.tool;
/*
*
*  This library is free software; you can redistribute it and/or modify it
*  under the terms of the GNU Lesser General Public License as published by the Free
*  Software Foundation; either version 3 of the License, or (at your option)
*  any later version.
*
*  This library is distributed in the hope that it will be useful, but
*  WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
*  or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
*  for more details.
*
*  You should have received a copy of the GNU Lesser General Public License
*  along with this program; if not, see <http://www.gnu.org/licenses/>.
*
*/

import com.martiansoftware.jsap.*;
import edu.nyu.tandon.experiments.ml.RawPostingsPrunedQuery;
import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.index.IndexIterator;
import it.unimi.di.big.mg4j.index.IndexReader;
import it.unimi.di.big.mg4j.search.DocumentIterator;
import it.unimi.di.big.mg4j.search.score.BM25Scorer;
import it.unimi.di.big.mg4j.search.score.Scorer;
import it.unimi.dsi.fastutil.BigList;
import it.unimi.dsi.fastutil.io.FastBufferedOutputStream;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.sux4j.io.FileLinesBigList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.Charset;
import java.util.HashSet;
import java.util.Set;

/**
 * Iterate through the entire index and prints out the index features
 * // -for each term in the index, prints term features: termID,docID,term-freq,doc-term-freq,bm25 to standard output
 *
 * @author Juan Rodriguez
 * @since 1.0
 */


public class PrunedIndexFeatures {

    static public Set<Long> DocumentList;                  // document pruning
    static public LongArrayList globalTermFrequencies;     // posting pruning

    private static void loadGlobalTermFrequencies(final String basename) throws Exception {
        globalTermFrequencies.clear();
        globalTermFrequencies.trim();
        String line;
        InputBitStream tf = new InputBitStream(basename);
        int n = 0;
        while (tf.hasNext()) {
            try {
                globalTermFrequencies.add(tf.readLongGamma());
            } catch (IOException ex) {
                break;
            }
        }
    }

    private static void loadDocumentPrunedList(final String basename, final int threshod) throws Exception {
        DocumentList.clear();
        String line;
        BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(basename), Charset.forName("UTF-8")));
        int n = 0;
        while ((line = br.readLine()) != null) {
            DocumentList.add(Long.parseLong(line));
            if (n++ > threshod) break;
        }
    }

    public static void main(String arg[]) throws Exception {

        SimpleJSAP jsap = new SimpleJSAP(RawPostingsPrunedQuery.class.getName(), "Output an index features",
                new Parameter[]{
                        new FlaggedOption("titleFile", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'T', "title-file", "A file of newline-separated, UTF-8 titles (will override collection titles if specified)."),
                        new FlaggedOption("divert", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'd', "divert", "output file"),
                        new Switch("globalScoring", 'G', "globalScoring", "Enable global metric scoring"),
                        new Switch("trec", 'E', "trec", "trec"),
                        new Switch("verbose", 'v', "verbose", "verbose"),
                        new UnflaggedOption("basenameWeight", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.GREEDY, "The indices that the servlet will use. Indices are specified using their basename, optionally followed by a colon and a double representing the weight used to score results from that index. Indices without a specified weight are weighted 1.")
                });


        DocumentList = new HashSet<Long>();
        globalTermFrequencies = new LongArrayList();

        final JSAPResult jsapResult = jsap.parse(arg);
        if (jsap.messagePrinted()) return;

        final String[] basenameWeight = jsapResult.getStringArray("basenameWeight");
        final boolean verbose = jsapResult.getBoolean("verbose");

        long document = 0;
        double score = 0;

        Logger LOGGER = LoggerFactory.getLogger(PrunedIndexFeatures.class);
        Scorer scorer = new BM25Scorer();

        final BigList<? extends CharSequence> titleList = jsapResult.userSpecified("titleFile") ? new FileLinesBigList(jsapResult.getString("titleFile"), "UTF-8") :
                null;

        final boolean trecMode = jsapResult.userSpecified("trec");

        if (titleList == null && trecMode) {
            System.out.println("Error: title file must be specified when chosing trec mode");
            return;
        }

        // redirect output
        PrintStream output = (jsapResult.userSpecified("divert")) ?
                new PrintStream(new FastBufferedOutputStream(new FileOutputStream(jsapResult.getString("divert")))) :
                System.out;

        /** First we open our index. The booleans tell that we want random access to
         * the inverted lists, and we are going to use document sizes (for scoring--see below). */
        final Index text = Index.getInstance(basenameWeight[0], true, true);
        final IndexReader reader = text.getReader();
        BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(basenameWeight[0] + ".terms"), Charset.forName("UTF-8")));
        IndexIterator iterator;
        while ((iterator = reader.nextIterator()) != null) {
            // for each term in the index, extract the term features: termID,docID,term-freq,doc-term-freq,bm25
            iterator.term(br.readLine());
            scorer.wrap(iterator);
            while ((document = iterator.nextDocument()) != DocumentIterator.END_OF_LIST) {

                if (trecMode) {
                    if (verbose)
                        output.printf("%s,%s,%d,%d,%f\n", iterator.term(), titleList.get(document), iterator.frequency(), iterator.count(), scorer.score());
                    else
                        output.printf("%s,%s\n", iterator.term(), titleList.get(document));

                } else {
                    if (verbose)
                        output.printf("%d,%d,%d,%d,%f\n", iterator.termNumber(), document, iterator.frequency(), iterator.count(), scorer.score());
                    else
                        output.printf("%s,%s\n", iterator.termNumber(), document);
                }
            }
        }
        if (output != System.out) output.close();
    }
}
