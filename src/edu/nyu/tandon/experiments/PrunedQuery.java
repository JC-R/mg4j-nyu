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
import edu.nyu.tandon.query.PrunedQueryEngine;
import edu.nyu.tandon.query.Query;
import it.unimi.di.big.mg4j.document.AbstractDocumentSequence;
import it.unimi.di.big.mg4j.document.DocumentCollection;
import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.index.TermProcessor;
import it.unimi.di.big.mg4j.query.IntervalSelector;
import it.unimi.di.big.mg4j.query.SelectedInterval;
import it.unimi.di.big.mg4j.query.TextMarker;
import it.unimi.di.big.mg4j.query.parser.QueryParserException;
import it.unimi.di.big.mg4j.query.parser.SimpleParser;
import it.unimi.di.big.mg4j.search.DocumentIteratorBuilderVisitor;
import it.unimi.di.big.mg4j.search.score.BM25Scorer;
import it.unimi.di.big.mg4j.search.score.DocumentScoreInfo;
import it.unimi.di.big.mg4j.search.score.Scorer;
import it.unimi.di.big.mg4j.search.score.VignaScorer;
import it.unimi.di.big.mg4j.util.MG4JClassParser;
import it.unimi.dsi.Util;
import it.unimi.dsi.fastutil.BigList;
import it.unimi.dsi.fastutil.Hash;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.objects.*;
import it.unimi.dsi.lang.ObjectParser;
import it.unimi.dsi.sux4j.io.FileLinesBigList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;

/** A command-line interpreter to query indices using a pruned list of documents.
 *
 * <p>This class can be used to start a {@linkplain QueryEngine query engine}
 * from the command line. Optionally, it can
 * start a {@linkplain HttpQueryServer web server} that will serve the results in a
 * search-engine-like environment. Changes
 * to the query engine made on the command line will reflect on subsequent queries (also on the
 * web server). The web server access is fully multithreaded.
 *
 * <p>This class does not provide command-line history or editing: to get that effect,
 * we suggest to rely on some operating-system utility such as
 * <a href="http://utopia.knoware.nl/~hlub/uck/rlwrap/"><samp>rlwrap</samp></a>.
 *
 * <p><strong>Warning:</strong> This class is <strong>highly experimental</strong> (it is the
 * place that we tweak to experiment every kind of new indexing/ranking method).
 */
public class PrunedQuery extends Query {

    private static final Logger LOGGER = LoggerFactory.getLogger(PrunedQuery.class);

    public PrunedQuery(final PrunedQueryEngine queryEngine) {
        super(queryEngine);
    }

    @SuppressWarnings("unchecked")
    public static void main( final String[] arg ) throws Exception {

        SimpleJSAP jsap = new SimpleJSAP( PrunedQuery.class.getName(), "Loads indices relative to a collection, possibly loads the collection, and answers to queries.",
                new Parameter[] {
                        new FlaggedOption( "collection", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'c', "collection", "The collection of documents indexed by the given indices." ),
                        new FlaggedOption( "objectCollection", new ObjectParser( DocumentCollection.class, MG4JClassParser.PACKAGE ), JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'o', "object-collection", "An object specification describing a document collection." ),
                        new FlaggedOption( "titleList", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 't', "title-list", "A serialized big list of titles (will override collection titles if specified)." ),
                        new FlaggedOption( "titleFile", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'T', "title-file", "A file of newline-separated, UTF-8 titles (will override collection titles if specified)." ),
                        new FlaggedOption( "input", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'I', "input", "A file containing the input." ),
                        new Switch( "noSizes", 'n', "no-sizes", "Disable loading document sizes (they are necessary for BM25 scoring)." ),
                        new Switch( "http", 'h', "http", "Starts an HTTP query server." ),
                        new Switch( "verbose", 'v', "verbose", "Print full exception stack traces." ),
                        new FlaggedOption( "itemClass", MG4JClassParser.getParser(), JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'i', "item-class", "The class that will handle item display in the HTTP server." ),
                        new FlaggedOption( "itemMimeType", JSAP.STRING_PARSER, "text/html", JSAP.NOT_REQUIRED, 'm', "item-mime-type", "A MIME type suggested to the class handling item display in the HTTP server." ),
                        new FlaggedOption( "port", JSAP.INTEGER_PARSER, "4242", JSAP.NOT_REQUIRED, 'p', "port", "The port on localhost where the server will appear." ),
                        new UnflaggedOption( "basenameWeight", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.GREEDY, "The indices that the servlet will use. Indices are specified using their basename, optionally followed by a colon and a double representing the weight used to score results from that index. Indices without a specified weight are weighted 1." ),

                        new Switch( "noMplex", 'P', "noMplex", "Starts with multiplex disabled." ),
                        new FlaggedOption( "results", JSAP.INTEGER_PARSER, "1000", JSAP.NOT_REQUIRED, 'r', "results", "The # of results to display" ),
                        new FlaggedOption( "mode", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'M', "time", "The results display mode" ),

                        new FlaggedOption( "divert", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'd', "divert", "output file" ),
                        new Switch( "prune", 'e', "prune", "Enable pruned list" ),
                        new FlaggedOption( "prunelist", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'L', "prunelist", "prune list" ),
                        new FlaggedOption( "threshold", JSAP.INTEGER_PARSER, "100", JSAP.NOT_REQUIRED, 's', "threshold", "prune threshold percentage" )


                });


        final JSAPResult jsapResult = jsap.parse(arg);
        if ( jsap.messagePrinted() ) return;

        final DocumentCollection documentCollection = (DocumentCollection)(jsapResult.userSpecified( "collection" ) ? AbstractDocumentSequence.load( jsapResult.getString( "collection" ) ) :
                jsapResult.userSpecified( "objectCollection" ) ? jsapResult.getObject( "objectCollection" ): null );
        final BigList<? extends CharSequence> titleList = (BigList<? extends CharSequence>) (
                jsapResult.userSpecified( "titleList" ) ? BinIO.loadObject( jsapResult.getString( "titleList" ) ) :
                        jsapResult.userSpecified( "titleFile" ) ? new FileLinesBigList( jsapResult.getString( "titleFile" ), "UTF-8" ) :
                                null );
        final String[] basenameWeight = jsapResult.getStringArray("basenameWeight");
        final Object2ReferenceLinkedOpenHashMap<String,Index> indexMap = new Object2ReferenceLinkedOpenHashMap<String,Index>( Hash.DEFAULT_INITIAL_SIZE, .5f );
        final Reference2DoubleOpenHashMap<Index> index2Weight = new Reference2DoubleOpenHashMap<Index>();
        final boolean verbose = jsapResult.getBoolean( "verbose" );
        final boolean loadSizes = ! jsapResult.getBoolean( "noSizes" );
        PrunedQuery.loadIndicesFromSpec(basenameWeight, loadSizes, documentCollection, indexMap, index2Weight);

        final long numberOfDocuments = indexMap.values().iterator().next().numberOfDocuments;
        if ( titleList != null && titleList.size64() != numberOfDocuments )
            throw new IllegalArgumentException( "The number of titles (" + titleList.size64() + " and the number of documents (" + numberOfDocuments + ") do not match" );

        final Object2ObjectOpenHashMap<String,TermProcessor> termProcessors = new Object2ObjectOpenHashMap<String,TermProcessor>( indexMap.size() );
        for( String alias: indexMap.keySet() ) termProcessors.put( alias, indexMap.get( alias ).termProcessor );

        final SimpleParser simpleParser = new SimpleParser( indexMap.keySet(), indexMap.firstKey(), termProcessors );

        final Reference2ReferenceMap<Index, Object> index2Parser = new Reference2ReferenceOpenHashMap<Index, Object>();

        final PrunedQueryEngine queryEngine = new PrunedQueryEngine( simpleParser, new DocumentIteratorBuilderVisitor( indexMap, index2Parser, indexMap.get( indexMap.firstKey() ), MAX_STEMMING ), indexMap );

        queryEngine.setWeights( index2Weight );
        queryEngine.score( new Scorer[] { new BM25Scorer(), new VignaScorer() }, new double[] { 1, 1 } );
        // We set up an interval selector only if there is a collection for snippeting
        queryEngine.intervalSelector = documentCollection != null ? new IntervalSelector( 4, 40 ) : new IntervalSelector();

        queryEngine.multiplex = jsapResult.userSpecified("moPlex") ? jsapResult.getBoolean( "noMplex" ) : true;

        queryEngine.prunning = jsapResult.userSpecified("prune") ? true : false;
        if (queryEngine.prunning) {
            queryEngine.loadPrunedIndex(jsapResult.getString("prunelist"),(int)((jsapResult.getInt("threshold",100)/100.0)*(float)numberOfDocuments));
        }

        queryEngine.equalize( 1000 );

        PrunedQuery query = new PrunedQuery( queryEngine );
        query.displayMode = OutputType.TIME;
        query.maxOutput = jsapResult.getInt("results", 20000);
        query.interpretCommand("$score BM25Scorer");
        if (jsapResult.userSpecified("divert"))
            query.interpretCommand("$divert " + jsapResult.getObject("divert"));

        LOGGER.debug("Processing Query with pruned index");

        String q;

        System.err.println( "Welcome to the MG4J query class (setup with $mode snippet, $score BM25Scorer VignaScorer, $mplex on, $equalize 1000, $select " + ( documentCollection != null ?  "4 40" : "all" ) + ")" );
        System.err.println( "Please type $ for help." );

        String prompt = indexMap.keySet().toString() + ">";
        int n;

        try {
            final BufferedReader br = new BufferedReader( new InputStreamReader( jsapResult.userSpecified( "input" ) ? new FileInputStream( jsapResult.getString( "input") ) : System.in ) );
            final ObjectArrayList<DocumentScoreInfo<Reference2ObjectMap<Index,SelectedInterval[]>>> results = new ObjectArrayList<DocumentScoreInfo<Reference2ObjectMap<Index,SelectedInterval[]>>>();

            for ( ;; ) {
                System.out.print( prompt );
                q = br.readLine();
                if ( q == null ) {
                    System.err.println();
                    break; // CTRL-D
                }
                if ( q.length() == 0 ) continue;
                if ( q.charAt( 0 ) == '$' ) {
                    if ( ! query.interpretCommand( q ) ) break;
                    continue;
                }

                long time = -System.nanoTime();

                try {
                    n = queryEngine.process( q, 0, query.maxOutput, results );
                }
                catch( QueryParserException e ) {
                    if ( verbose ) e.getCause().printStackTrace( System.err );
                    else System.err.println( e.getCause() );
                    continue;
                }
                catch( Exception e ) {
                    if ( verbose ) e.printStackTrace( System.err );
                    else System.err.println( e );
                    continue;
                }

                time += System.nanoTime();
                query.output( results, documentCollection, titleList, TextMarker.TEXT_BOLDFACE );
                System.err.println( results.size() + " results; " + n + " documents examined; " + time / 1000000. + " ms; " + Util.format( ( n * 1000000000.0 ) / time ) + " documents/s, " + Util.format( time / (double)n ) + " ns/document" );
            }

        }
        finally {
            if ( query.output != System.out ) query.output.close();
        }
    }
}
