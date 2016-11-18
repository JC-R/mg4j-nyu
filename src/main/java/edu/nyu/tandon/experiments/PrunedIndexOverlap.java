package edu.nyu.tandon.experiments;
/*
 * MG4J: Managing Gigabytes for Java (big)
 *
 * Copyright (C) 2005-2015 Paolo Boldi and Sebastiano Vigna
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
import edu.nyu.tandon.query.HitsQueryEngine;
import edu.nyu.tandon.query.Query;
import edu.nyu.tandon.query.QueryEngine;
import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.index.IndexIterator;
import it.unimi.di.big.mg4j.index.TermProcessor;
import it.unimi.di.big.mg4j.query.HttpQueryServer;
import it.unimi.di.big.mg4j.query.parser.QueryParserException;
import it.unimi.di.big.mg4j.query.parser.SimpleParser;
import it.unimi.di.big.mg4j.search.DocumentIteratorBuilderVisitor;
import it.unimi.di.big.mg4j.search.score.BM25Scorer;
import it.unimi.di.big.mg4j.search.score.DocumentScoreInfo;
import it.unimi.dsi.fastutil.BigList;
import it.unimi.dsi.fastutil.BigListIterator;
import it.unimi.dsi.fastutil.Hash;
import it.unimi.dsi.fastutil.io.FastBufferedOutputStream;
import it.unimi.dsi.fastutil.longs.*;
import it.unimi.dsi.fastutil.objects.*;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.sux4j.io.FileLinesBigList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

//import it.unimi.di.big.mg4j.query.*;

/**
 * A command-line interpreter to query indices.
 * <p>
 * <p>This class can be used to start a {@linkplain QueryEngine query engine}
 * from the command line. Optionally, it can
 * start a {@linkplain HttpQueryServer web server} that will serve the results in a
 * search-engine-like environment. Changes
 * to the query engine made on the command line will reflect on subsequent queries (also on the
 * web server). The web server access is fully multithreaded.
 * <p>
 * <p>This class does not provide command-line history or editing: to get that effect,
 * we suggest to rely on some operating-system utility such as
 * <a href="http://utopia.knoware.nl/~hlub/uck/rlwrap/"><samp>rlwrap</samp></a>.
 * <p>
 * <p><strong>Warning:</strong> This class is <strong>highly experimental</strong> (it is the
 * place that we tweak to experiment every kind of new indexing/ranking method).
 */
public class PrunedIndexOverlap extends Query {

    private static final Logger LOGGER = LoggerFactory.getLogger(PrunedIndexOverlap.class);
    static PrintStream output;
    static Index prunedIndex;
    static boolean trecMode, verbose;
    static float docsKept = 0;
    static float postingsKept = 0;
    static BigList<? extends CharSequence> baseTitleList;
    static Object2LongMap<MutableString> prunedTitleMap;

    public PrunedIndexOverlap(final QueryEngine queryEngine) {
        super(queryEngine);
    }

    @SuppressWarnings("unchecked")
    public static void main(final String[] arg) throws Exception {

        SimpleJSAP jsap = new SimpleJSAP(PrunedIndexOverlap.class.getName(), "Loads indices relative to a collection, possibly loads the collection, and answers to queries.",
                new Parameter[]{
                        new FlaggedOption("input", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'I', "input", "A file containing the input."),
                        new Switch("noSizes", 'n', "no-sizes", "Disable loading document sizes (they are necessary for BM25 scoring)."),
                        new Switch("noMplex", 'P', "noMplex", "Starts with multiplex disabled."),
                        new FlaggedOption("results", JSAP.INTEGER_PARSER, "1000", JSAP.NOT_REQUIRED, 'r', "results", "The # of results to display"),
                        new FlaggedOption("basetitleFile", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'T', "base-title-file", "A file of newline-separated, UTF-8 titles (will override collection titles if specified)."),
                        new FlaggedOption("prunedtitleFile", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 't', "pruned-title-file", "A file of newline-separated, UTF-8 titles (will override collection titles if specified)."),
                        new FlaggedOption("divert", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'd', "divert", "output file"),
                        new Switch("trec", 'E', "trec", "trec"),
                        new Switch("verbose", 'v', "verbose", "verbose"),
                        new FlaggedOption("index", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'i', "index", "index to compare against baseline"),
                        new UnflaggedOption("basenameWeight", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.GREEDY, "The indices that the servlet will use. Indices are specified using their basename, optionally followed by a colon and a double representing the weight used to score results from that index. Indices without a specified weight are weighted 1.")
                });

        final JSAPResult jsapResult = jsap.parse(arg);
        if (jsap.messagePrinted()) return;

        baseTitleList = (BigList<? extends CharSequence>) new FileLinesBigList(jsapResult.getString("basetitleFile"), "UTF-8");

        BigList<? extends CharSequence> prunedTitleList;
        prunedTitleList = (BigList<? extends CharSequence>) new FileLinesBigList(jsapResult.getString("prunedtitleFile"), "UTF-8");
        prunedTitleMap = new Object2LongOpenHashMap<MutableString>(prunedTitleList.size());
        BigListIterator it = prunedTitleList.listIterator();
        int n = 0;
        while (it.hasNext()) {
            prunedTitleMap.put((MutableString) it.next(), n++);
        }


        trecMode = jsapResult.userSpecified("trec") ? true : false;
        verbose = jsapResult.userSpecified("verbose") ? true : false;

        // redirect output
        output = (jsapResult.userSpecified("divert")) ?
                new PrintStream(new FastBufferedOutputStream(new FileOutputStream(jsapResult.getString("divert")))) :
                System.out;

        final String[] basenameWeight = jsapResult.getStringArray("basenameWeight");
        final Object2ReferenceLinkedOpenHashMap<String, Index> indexMap = new Object2ReferenceLinkedOpenHashMap<String, Index>(Hash.DEFAULT_INITIAL_SIZE, .5f);
        final Reference2DoubleOpenHashMap<Index> index2Weight = new Reference2DoubleOpenHashMap<Index>();
        final boolean loadSizes = !jsapResult.getBoolean("noSizes");

        PrunedIndexOverlap.loadIndicesFromSpec(basenameWeight, loadSizes, null, indexMap, index2Weight);

        final Object2ObjectOpenHashMap<String, TermProcessor> termProcessors = new Object2ObjectOpenHashMap<String, TermProcessor>(indexMap.size());
        for (String alias : indexMap.keySet()) termProcessors.put(alias, indexMap.get(alias).termProcessor);

        final SimpleParser simpleParser = new SimpleParser(indexMap.keySet(), indexMap.firstKey(), termProcessors);

        final Reference2ReferenceMap<Index, Object> index2Parser = new Reference2ReferenceOpenHashMap<Index, Object>();

        final HitsQueryEngine queryEngine = new HitsQueryEngine(simpleParser, new DocumentIteratorBuilderVisitor(indexMap, index2Parser, indexMap.get(indexMap.firstKey()), MAX_STEMMING), indexMap);
        queryEngine.setWeights(index2Weight);
        queryEngine.score(new BM25Scorer(1.2, 0.3));

        queryEngine.multiplex = jsapResult.userSpecified("moPlex") ? jsapResult.getBoolean("noMplex") : true;
        queryEngine.intervalSelector = null;

        PrunedIndexOverlap query = new PrunedIndexOverlap(queryEngine);

        if (jsapResult.userSpecified("divert"))
            query.interpretCommand("$divert " + jsapResult.getObject("divert"));

        query.maxOutput = jsapResult.getInt("results", 10000);

        String q;
        n = 0;
        int queryCount = 0;

        // pruned index (subset of the baseline index)
        prunedIndex = Index.getInstance(jsapResult.getString("index"), true, false);

        String prompt = indexMap.keySet().toString() + ">";

        try {
            final BufferedReader br = new BufferedReader(new InputStreamReader(jsapResult.userSpecified("input") ? new FileInputStream(jsapResult.getString("input")) : System.in));
            final ObjectArrayList<DocumentScoreInfo<ObjectArrayList<Byte>>> results = new ObjectArrayList<DocumentScoreInfo<ObjectArrayList<Byte>>>();

            for (; ; ) {
                if (!jsapResult.userSpecified("input")) System.err.print(prompt);
                q = br.readLine();
                if (q == null) {
                    System.err.println();
                    break; // CTRL-D
                }
                if (q.length() == 0) continue;
                if (q.charAt(0) == '$') {
                    if (!query.interpretCommand(q)) break;
                    continue;
                }

                long time = -System.nanoTime();
                try {
                    n = queryEngine.process(q, 0, query.maxOutput, results);
                } catch (QueryParserException e) {
                    System.err.println(e.getCause());
                    queryCount++;
                    continue;
                } catch (Exception e) {
                    System.err.println(e);
                    queryCount++;
                    continue;
                }
                time += System.nanoTime();

                query.output(queryCount, results);

                queryCount++;
            }
        } finally {
            if (query.output != System.out) query.output.close();
        }
        LOGGER.debug("Processed " + queryCount + " queries");
        System.out.printf("%.4f,%.4f", docsKept / queryCount, postingsKept / queryCount);

    }

    /**
     * Scores the given document iterator and produces scored output.
     *
     * @param results an iterator returning instances of {@link DocumentScoreInfo}.
     * @return the number of documents scanned.
     */
    @SuppressWarnings("boxing")
    public int output(final int queryNumber,
                      final ObjectArrayList<DocumentScoreInfo<ObjectArrayList<Byte>>> results) throws IOException {

        int i, j;
        long baseDocID = 0, prunedDocID = 0;

        DocumentScoreInfo<ObjectArrayList<Byte>> dsi;
        String term;
        IndexIterator prunedIterator;

        // compute total result docs
        long count = results.size();

        int baseDocs = 0;
        int basePostings = 0;

        boolean docInPrunedIndex = false;
        int prunedDocs = 0;
        int prunedPostings = 0;

        for (i = 0; i < count; i++) {

            dsi = results.get(i);

            // baseline stats
            baseDocID = (int) dsi.document;
            baseDocs++;
            basePostings += dsi.info.size();

            docInPrunedIndex = false;

            // doc in pruned index? look up via doc title (ID numbers are local to the index)
            try {
                prunedDocID = prunedTitleMap.get(baseTitleList.get(baseDocID));
            } catch (NullPointerException e) {
                continue;
            }

            // find doc & postings kept in the pruned index
            for (j = 0; j < dsi.info.size(); j++) {

                // find the list for this term
                term = ((BM25Scorer) queryEngine.scorer).flatIndexIterator[dsi.info.get(j)].term();
                prunedIterator = prunedIndex.documents(term);
                if (prunedIterator == null) continue;           // this term not in pruned index

                // posting in pruned index?
                if (prunedIterator.skipTo(prunedDocID) == prunedDocID) {
                    docInPrunedIndex = true;
                    prunedPostings++;
                    if (verbose && trecMode)
                        output.printf("%d,%s,%s,%d\n", queryNumber, term, baseTitleList.get(baseDocID), i + 1);
                }
                prunedIterator.dispose();
            }
            if (docInPrunedIndex)
                prunedDocs++;
        }

        float dk = (baseDocs > 0) ? (float) prunedDocs / baseDocs : 0;
        float pk = (basePostings > 0) ? (float) prunedPostings / basePostings : 0;

        docsKept += dk;
        postingsKept += pk;

        if (verbose)
            System.out.printf("%d,%d,%d,%.3f,%d,%d,%.3f\n",
                    queryNumber,
                    prunedDocs,
                    baseDocs,
                    dk,
                    prunedPostings,
                    basePostings,
                    pk);

        return prunedPostings;
    }

}
