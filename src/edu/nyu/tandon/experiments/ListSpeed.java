package edu.nyu.tandon.experiments;

import com.martiansoftware.jsap.*;
import edu.nyu.tandon.query.Query;
import edu.nyu.tandon.query.QueryEngine;
import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.index.IndexIterator;
import it.unimi.di.big.mg4j.index.TermProcessor;
import it.unimi.di.big.mg4j.query.IntervalSelector;
import it.unimi.di.big.mg4j.query.parser.SimpleParser;
import it.unimi.di.big.mg4j.search.DocumentIteratorBuilderVisitor;
import it.unimi.di.big.mg4j.search.score.Scorer;
import it.unimi.dsi.Util;
import it.unimi.dsi.fastutil.BigList;
import it.unimi.dsi.fastutil.Hash;
import it.unimi.dsi.fastutil.objects.*;

import java.util.Arrays;

import static edu.nyu.tandon.utils.utils.rand;
import static it.unimi.di.big.mg4j.search.DocumentIterator.END_OF_LIST;

/**
 * Created by RodriguJ on 6/10/2015.
 */
public class ListSpeed {


    @SuppressWarnings("unchecked")
    public static void main(final String[] arg) throws Exception {

        SimpleJSAP jsap = new SimpleJSAP(ListSpeed.class.getName(), "Loads random list, primes, and measure random posting access times",
                new Parameter[]{
                        new UnflaggedOption("basenameWeight", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.GREEDY, "The indices that the servlet will use. Indices are specified using their basename, optionally followed by a colon and a double representing the weight used to score results from that index. Indices without a specified weight are weighted 1."),
                        new FlaggedOption("random", JSAP.INTEGER_PARSER, "1000", JSAP.NOT_REQUIRED, 'r', "random", "The # of postings to measure"),
                        new FlaggedOption("samples", JSAP.INTEGER_PARSER, "10", JSAP.NOT_REQUIRED, 's', "samples", "The # of terms to measure")
                });

        final JSAPResult jsapResult = jsap.parse(arg);
        if (jsap.messagePrinted()) return;

        final BigList<? extends CharSequence> titleList = null;
        final String[] basenameWeight = jsapResult.getStringArray("basenameWeight");

        final Object2ReferenceLinkedOpenHashMap<String, Index> indexMap = new Object2ReferenceLinkedOpenHashMap<String, Index>(Hash.DEFAULT_INITIAL_SIZE, .5f);
        final Reference2DoubleOpenHashMap<Index> index2Weight = new Reference2DoubleOpenHashMap<Index>();

        // load index
        Query.loadIndicesFromSpec(basenameWeight, true, null, indexMap, index2Weight);

        // get stats
        final long numberOfDocuments = indexMap.values().iterator().next().numberOfDocuments;
        final long numberOfTerms = indexMap.values().iterator().next().numberOfTerms;
        final long numberOfSamples = jsapResult.getInt("samples", (int) numberOfDocuments);

        final Object2ObjectOpenHashMap<String, TermProcessor> termProcessors = new Object2ObjectOpenHashMap<String, TermProcessor>(indexMap.size());
        for (String alias : indexMap.keySet()) termProcessors.put(alias, indexMap.get(alias).termProcessor);
        final SimpleParser simpleParser = new SimpleParser(indexMap.keySet(), indexMap.firstKey(), termProcessors);
        final Reference2ReferenceMap<Index, Object> index2Parser = new Reference2ReferenceOpenHashMap<Index, Object>();
        final QueryEngine queryEngine = new QueryEngine(simpleParser, new DocumentIteratorBuilderVisitor(indexMap, index2Parser, indexMap.get(indexMap.firstKey()), 2048), indexMap);
        queryEngine.setWeights(index2Weight);

        queryEngine.score(new Scorer[]{}, new double[]{1, 1});
        queryEngine.intervalSelector = new IntervalSelector();
        queryEngine.multiplex = false;

        Query query = new Query(queryEngine);
        query.displayMode = Query.OutputType.TIME;
        query.maxOutput = jsapResult.getInt("results", (int) numberOfDocuments);
        query.interpretCommand("$mode time");

        System.err.println("MG4J experiment - posting access times.");

        // bootstrap the system
        long[] terms = new long[(int) numberOfSamples];
        for (int i = 0; i < terms.length; ) {
            // ge a list, add if above theshold
            terms[i] = rand(0, (int) numberOfTerms);
            IndexIterator list = indexMap.values().iterator().next().documents(terms[i]);
            list.dispose();
            if (list.frequency() >= 10000) {
                i++;
                System.err.print("\rAdding random terms: " + i);
            }
        }
        System.err.println();
        double totSequential = 0;
        double totRandom = 0;
        long totSeqPostings = 0;
        long totRandPostings = 0;

        for (int nextTerm = 0; nextTerm < terms.length; nextTerm++) {
            int n = 0;
            long document = 0;
            long count = 0;

            try {

                // get pointer to the term list
                IndexIterator list = indexMap.values().iterator().next().documents(terms[nextTerm]);
                if (list.frequency() == 0) continue;
                totSeqPostings += list.frequency();
                list.dispose();

                double total = 0;

                // sequential access metrics; first pass to prime the system
                for (int i = 0; i < 101; i++) {
                    list = indexMap.values().iterator().next().documents(terms[nextTerm]);
                    long time = -System.nanoTime();
                    // scan entire list
                    while ((document = list.nextDocument()) != END_OF_LIST) ;
                    time += System.nanoTime();
                    if (i > 0) total += time;
                    list.dispose();
                }
                total /= 100;
                totSequential += total;
//                System.err.println();
                System.out.println("Sequential access - term: " + terms[nextTerm] + ", " + list.frequency() + " postings; " + total / 1000000. + " ms; " + Util.format((list.frequency() * 1000000000.0) / total) + " postings/s, " + Util.format(total / (double) list.frequency()) + " ns/posting");

                // random access metrics
                list = indexMap.values().iterator().next().documents(terms[nextTerm]);
                long[] postings = new long[(int) list.frequency()];
                n = 0;
                while ((document = list.nextDocument()) != END_OF_LIST) postings[n++] = document;
                n = (n < 25) ? n : 25;
                long[] test = new long[n];
                for (int i = 0; i < n; i++) test[i] = postings[rand(0, (int) list.frequency() - 1)];
                Arrays.sort(test);
                list.dispose();
                totRandPostings += n;

                total = 0;
                for (int i = 0; i < 101; i++) {
                    list = indexMap.values().iterator().next().documents(terms[nextTerm]);
                    long time = -System.nanoTime();
                    for (int j = 0; j < n; j++) {
                        list.skipTo(test[j]);
                        document = list.nextDocument();
                    }
                    time += System.nanoTime();
                    if (i > 0) total += time;
                    list.dispose();
                }
                total /= 100;
                totRandom += total;
                System.out.println("Random access - term: " + terms[nextTerm] + " " + n + " postings; " + total / 1000000. + " ms; " + Util.format((n * 1000000000.0) / total) + " postings/s, " + Util.format(total / (double) n) + " ns/posting");

            } catch (Exception e) {
                System.err.println(e);
            }
        }
        System.out.println();
        System.out.println("Global sequential access: " + totSeqPostings + " postings; " + totSequential / 1000000. + " ms; " + Util.format((totSeqPostings * 1000000000.0) / totSequential) + " postings/s, " + Util.format(totSequential / (double) totSeqPostings) + " ns/posting");
        System.out.println("Global Random access: " + totRandPostings + " postings; " + totRandom / 1000000. + " ms; " + Util.format((totRandPostings * 1000000000.0) / totRandom) + " postings/s, " + Util.format(totRandom / (double) totRandPostings) + " ns/posting");

    }

}
