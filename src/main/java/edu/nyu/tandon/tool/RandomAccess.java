package edu.nyu.tandon.tool;

import com.martiansoftware.jsap.*;
import edu.nyu.tandon.tool.ListSpeed;
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
import it.unimi.dsi.fastutil.Hash;
import it.unimi.dsi.fastutil.objects.*;

import java.util.Set;
import java.util.TreeSet;

import static edu.nyu.tandon.utils.Utils.rand;
import static it.unimi.di.big.mg4j.search.DocumentIterator.END_OF_LIST;

/**
 * Created by RodriguJ on 6/11/2015.
 */
public class RandomAccess {

    @SuppressWarnings("unchecked")
    public static void main(final String[] arg) throws Exception {

        SimpleJSAP jsap = new SimpleJSAP(
                ListSpeed.class.getName(),
                "Loads random list, primes, and measure random posting access times",
                new Parameter[]{
                        new UnflaggedOption("basenameWeight", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.GREEDY, "The indices that the servlet will use. Indices are specified using their basename, optionally followed by a colon and a double representing the weight used to score results from that index. Indices without a specified weight are weighted 1."),
                        new FlaggedOption("random", JSAP.INTEGER_PARSER, "1000", JSAP.NOT_REQUIRED, 'r', "random", "The # of postings_Global to measure"),
                        new FlaggedOption("threshold", JSAP.INTEGER_PARSER, "100000", JSAP.NOT_REQUIRED, 't', "threshold", "The min # terms in a document"),
                        new FlaggedOption("samples", JSAP.INTEGER_PARSER, "1", JSAP.NOT_REQUIRED, 's', "samples", "The # of terms to measure")
                });

        final JSAPResult jsapResult = jsap.parse(arg);
        if (jsap.messagePrinted()) return;

        final String[] basenameWeight = jsapResult.getStringArray("basenameWeight");

        final Object2ReferenceLinkedOpenHashMap<String, Index> indexMap = new Object2ReferenceLinkedOpenHashMap<String, Index>(Hash.DEFAULT_INITIAL_SIZE, .5f);
        final Reference2DoubleOpenHashMap<Index> index2Weight = new Reference2DoubleOpenHashMap<Index>();

        // load index
        // ----------
        Query.loadIndicesFromSpec(basenameWeight, true, null, indexMap, index2Weight);

        // get stats
        final long numberOfDocuments = indexMap.values().iterator().next().numberOfDocuments;
        final long numberOfTerms = indexMap.values().iterator().next().numberOfTerms;
        final long numberOfSamples = jsapResult.getInt("samples", (int) numberOfDocuments);
        final long numberOPostings = jsapResult.getInt("random", 1000);

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
        int threshold = jsapResult.getInt("threshold", 100000);
        query.interpretCommand("$mode time");

        System.err.println("MG4J experiment - posting access times.");

        // seed with random documents
        Set<Integer> set = new TreeSet<Integer>();
        for (int i = 0; i < numberOPostings; i++) {
            set.add(rand(0, (int) numberOfDocuments - 1));
            System.err.print("\rAdding random postings_Global: " + i);
        }
        Integer[] docs = set.toArray(new Integer[0]);

        // find 10 terms with at least t postings_Global
        long[] terms = new long[10];
        int numTerms = 0;
        for (int i = 0; i < numberOfTerms && numTerms < 10; i++) {
            System.err.print("\rSearching terms: " + i);
            IndexIterator list = indexMap.values().iterator().next().documents(i);
            list.dispose();
            if (list.frequency() >= threshold) {
                terms[numTerms++] = list.termNumber();
            }
        }
        if (numTerms == 0) return;

        System.err.println();
        double totSequential = 0;
        double totRandom = 0;
        long totSeqPostings = 0;
        long totRandPostings = 0;
        double totHits = 0;
        double totMisses = 0;

        int n = 0;
        long document = 0;
        long count = 0;

        for (int k = 0; k < numTerms; k++) {

            try {

                // get pointer to the term list
                IndexIterator list = indexMap.values().iterator().next().documents(terms[k]);
                if (list.frequency() == 0) return;
                totSeqPostings += list.frequency();
                list.dispose();

                double total = 0;

                // sequential access metrics; first pass to prime the system
                for (int i = 0; i < 11; i++) {
                    list = indexMap.values().iterator().next().documents(terms[k]);
                    long time = -System.nanoTime();
                    // scan entire list
                    while ((document = list.nextDocument()) != END_OF_LIST) ;
                    time += System.nanoTime();
                    if (i > 0) total += time;
                    list.dispose();
                }
                total /= 10;
                totSequential += total;
                System.out.println("Term " + k + ": " + list.frequency() + " postings_Global");
                System.out.println("Sequential access:  " + total / 1000000. + " ms,  " + Util.format((list.frequency() * 1000000000.0) / total) + " postings_Global/s,   " + Util.format(total / (double) list.frequency()) + " ns/posting");

                // random access metrics
                total = 0;
                long hits = 0;
                long misses = 0;
                totRandPostings += docs.length;
                for (int i = 0; i < 11; i++) {

                    list = indexMap.values().iterator().next().documents(terms[k]);
                    document = -1;
                    if (i > 0) {
                        long time = -System.nanoTime();
                        for (int j = 0; j < docs.length && document != END_OF_LIST; j++)
                            document = list.skipTo(docs[j]);
                        time += System.nanoTime();
                        total += time;
                    } else {
                        for (int j = 0; j < docs.length && document != END_OF_LIST; j++) {
                            document = list.skipTo(docs[j]);
                            if (document == docs[j]) hits++;
                            else misses++;
                        }
                        list.dispose();
                    }
                }
                total /= 10;
                totRandom += total;
                totHits += hits;
                totMisses += misses;

                System.out.println("Random access:  " + docs.length + " lookups,  " + hits + " hits,  " + misses + " misses,  " + total / 1000000. + " ms,  " + Util.format((docs.length * 1000000000.0) / total) + " postings_Global/s, " + Util.format(total / (double) docs.length) + " ns/posting");
                System.out.println();
            } catch (Exception e) {
                System.err.println(e);
            }
        }

        totHits /= numTerms;
        totMisses /= numTerms;
        totSequential /= numTerms;
        totRandom /= numTerms;
        totSeqPostings /= numTerms;
        totRandPostings /= numTerms;

        System.out.println();
        System.out.println();
        System.out.println("Average: " + +totSeqPostings + " postings_Global");
        System.out.println("Sequential access: " + totSequential / 1000000. + " ms; " + Util.format((totSeqPostings * 1000000000.0) / totSequential) + " postings_Global/s, " + Util.format(totSequential / (double) totSeqPostings) + " ns/posting");
        System.out.println("Random access: " + docs.length + " lookups,  " + totHits + " hits,  " + totMisses + " misses,  " + totRandom / 1000000. + " ms,  " + Util.format((totRandPostings * 1000000000.0) / totRandom) + " postings_Global/s, " + Util.format(totRandom / (double) docs.length) + " ns/posting");

    }

}

