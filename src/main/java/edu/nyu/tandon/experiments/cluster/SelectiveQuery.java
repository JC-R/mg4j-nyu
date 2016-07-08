package edu.nyu.tandon.experiments.cluster;

import com.martiansoftware.jsap.*;
import edu.nyu.tandon.query.Query;
import edu.nyu.tandon.query.QueryEngine;
import edu.nyu.tandon.query.SelectiveQueryEngine;
import it.unimi.di.big.mg4j.document.AbstractDocumentSequence;
import it.unimi.di.big.mg4j.document.DocumentCollection;
import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.index.TermProcessor;
import it.unimi.di.big.mg4j.query.SelectedInterval;
import it.unimi.di.big.mg4j.query.TextMarker;
import it.unimi.di.big.mg4j.query.parser.QueryParserException;
import it.unimi.di.big.mg4j.query.parser.SimpleParser;
import it.unimi.di.big.mg4j.search.DocumentIteratorBuilderVisitor;
import it.unimi.di.big.mg4j.search.score.DocumentScoreInfo;
import it.unimi.di.big.mg4j.util.MG4JClassParser;
import it.unimi.dsi.Util;
import it.unimi.dsi.fastutil.BigList;
import it.unimi.dsi.fastutil.Hash;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.objects.*;
import it.unimi.dsi.lang.ObjectParser;
import it.unimi.dsi.sux4j.io.FileLinesBigList;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class SelectiveQuery extends Query {

    public SelectiveQuery(QueryEngine queryEngine) {
        super(queryEngine);
    }

    public static void main(final String[] arg) throws Exception {

        SimpleJSAP jsap = new SimpleJSAP(Query.class.getName(), "Loads indices relative to a collection, possibly loads the collection, and answers to queries.",
                new Parameter[]{
                        new FlaggedOption("collection", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'c', "collection", "The collection of documents indexed by the given indices."),
                        new FlaggedOption("objectCollection", new ObjectParser(DocumentCollection.class, MG4JClassParser.PACKAGE), JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'o', "object-collection", "An object specification describing a document collection."),
                        new FlaggedOption("titleList", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 't', "title-list", "A serialized big list of titles (will override collection titles if specified)."),
                        new FlaggedOption("titleFile", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'T', "title-file", "A file of newline-separated, UTF-8 titles (will override collection titles if specified)."),
                        new FlaggedOption("input", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'I', "input", "A file containing the input."),
                        new Switch("noSizes", 'n', "no-sizes", "Disable loading document sizes (they are necessary for BM25 scoring)."),
                        new Switch("http", 'h', "http", "Starts an HTTP query server."),
                        new Switch("verbose", 'v', "verbose", "Print full exception stack traces."),
                        new FlaggedOption("itemClass", MG4JClassParser.getParser(), JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'i', "item-class", "The class that will handle item display in the HTTP server."),
                        new FlaggedOption("itemMimeType", JSAP.STRING_PARSER, "text/html", JSAP.NOT_REQUIRED, 'm', "item-mime-type", "A MIME type suggested to the class handling item display in the HTTP server."),
                        new FlaggedOption("port", JSAP.INTEGER_PARSER, "4242", JSAP.NOT_REQUIRED, 'p', "port", "The port on localhost where the server will appear."),
                        new UnflaggedOption("basenameWeight", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.GREEDY, "The indices that the servlet will use. Indices are specified using their basename, optionally followed by a colon and a double representing the weight used to score results from that index. Indices without a specified weight are weighted 1."),

                        new Switch("noMplex", 'P', "noMplex", "Starts with multiplex disabled."),
                        new FlaggedOption("results", JSAP.INTEGER_PARSER, "1000", JSAP.NOT_REQUIRED, 'r', "results", "The # of results to display"),
                        new FlaggedOption("mode", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'M', "time", "The results display mode"),

                        new FlaggedOption("csi", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'C', "csi", "Central sample index")
                });


        final JSAPResult jsapResult = jsap.parse(arg);
        if (jsap.messagePrinted()) return;

        final DocumentCollection documentCollection = (DocumentCollection) (jsapResult.userSpecified("collection") ? AbstractDocumentSequence.load(jsapResult.getString("collection")) :
                jsapResult.userSpecified("objectCollection") ? jsapResult.getObject("objectCollection") : null);
        final BigList<? extends CharSequence> titleList = (BigList<? extends CharSequence>) (
                jsapResult.userSpecified("titleList") ? BinIO.loadObject(jsapResult.getString("titleList")) :
                        jsapResult.userSpecified("titleFile") ? new FileLinesBigList(jsapResult.getString("titleFile"), "UTF-8") :
                                null);
        final String[] basenameWeight = jsapResult.getStringArray("basenameWeight");
        final Object2ReferenceLinkedOpenHashMap<String, Index> indexMap = new Object2ReferenceLinkedOpenHashMap<String, Index>(Hash.DEFAULT_INITIAL_SIZE, .5f);
        final Reference2DoubleOpenHashMap<Index> index2Weight = new Reference2DoubleOpenHashMap<Index>();
        final boolean verbose = jsapResult.getBoolean("verbose");
        final boolean loadSizes = !jsapResult.getBoolean("noSizes");
        Query.loadIndicesFromSpec(basenameWeight, loadSizes, documentCollection, indexMap, index2Weight);

        final long numberOfDocuments = indexMap.values().iterator().next().numberOfDocuments;
        if (titleList != null && titleList.size64() != numberOfDocuments)
            throw new IllegalArgumentException("The number of titles (" + titleList.size64() + " and the number of documents (" + numberOfDocuments + ") do not match");

        final Object2ObjectOpenHashMap<String, TermProcessor> termProcessors = new Object2ObjectOpenHashMap<String, TermProcessor>(indexMap.size());
        for (String alias : indexMap.keySet()) termProcessors.put(alias, indexMap.get(alias).termProcessor);

        final SimpleParser simpleParser = new SimpleParser(indexMap.keySet(), indexMap.firstKey(), termProcessors);

        final Reference2ReferenceMap<Index, Object> index2Parser = new Reference2ReferenceOpenHashMap<Index, Object>();
        /*
        // Fetch parsers for payload-based fields.
		for( Index index: indexMap.values() ) if ( index.hasPayloads ) {
			if ( index.payload.getClass() == DatePayload.class ) index2Parser.put( index, DateFormat.getDateInstance( DateFormat.SHORT, Locale.UK ) );
		}
		*/

        final SelectiveQueryEngine queryEngine = new SelectiveQueryEngine(simpleParser, new DocumentIteratorBuilderVisitor(indexMap, index2Parser, indexMap.get(indexMap.firstKey()), MAX_STEMMING), indexMap,
                basenameWeight[0], jsapResult.getString("csi"));

        SelectiveQuery query = new SelectiveQuery(queryEngine);
        query.displayMode = Query.OutputType.TIME;

        query.maxOutput = jsapResult.getInt("results", 1000);

        query.interpretCommand("$score BM25Scorer");

        String q;

        System.err.println("Welcome to the MG4J query class (setup with $mode snippet, $score BM25Scorer VignaScorer, $mplex on, $equalize 1000, $select " + (documentCollection != null ? "4 40" : "all") + ")");
        System.err.println("Please type $ for help.");

        String prompt = indexMap.keySet().toString() + ">";
        int n;

        try {
            final BufferedReader br = new BufferedReader(new InputStreamReader(jsapResult.userSpecified("input") ? new FileInputStream(jsapResult.getString("input")) : System.in));
            final ObjectArrayList<DocumentScoreInfo<Reference2ObjectMap<Index, SelectedInterval[]>>> results = new ObjectArrayList<DocumentScoreInfo<Reference2ObjectMap<Index, SelectedInterval[]>>>();

            for (; ; ) {
                if (query.displayMode != Query.OutputType.TREC) System.out.print(prompt);
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
                    if (verbose) e.getCause().printStackTrace(System.err);
                    else System.err.println(e.getCause());
                    continue;
                } catch (Exception e) {
                    if (verbose) e.printStackTrace(System.err);
                    else System.err.println(e);
                    continue;
                }

                time += System.nanoTime();
                query.output(results, documentCollection, titleList, TextMarker.TEXT_BOLDFACE);
                System.err.println(results.size() + " results; " + n + " documents examined; " + time / 1000000. + " ms; " + Util.format((n * 1000000000.0) / time) + " documents/s, " + Util.format(time / (double) n) + " ns/document");
            }

        } finally {
            if (query.output != System.out) query.output.close();
        }
    }

}
