package edu.nyu.tandon.experiments.ml.StaticPruning;

import com.martiansoftware.jsap.*;
import edu.nyu.tandon.index.cluster.PostingPruningStrategy;
import edu.nyu.tandon.query.PrunedQueryEngine;
import edu.nyu.tandon.query.Query;
import edu.nyu.tandon.search.score.BM25PrunedScorer;
import it.unimi.di.big.mg4j.document.AbstractDocumentSequence;
import it.unimi.di.big.mg4j.document.Document;
import it.unimi.di.big.mg4j.document.DocumentCollection;
import it.unimi.di.big.mg4j.document.DocumentFactory;
import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.index.TermProcessor;
import it.unimi.di.big.mg4j.query.*;
import it.unimi.di.big.mg4j.query.parser.QueryParserException;
import it.unimi.di.big.mg4j.query.parser.SimpleParser;
import it.unimi.di.big.mg4j.search.DocumentIteratorBuilderVisitor;
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

import java.io.*;
import java.util.Arrays;
import java.util.Comparator;

// TODO: this entire code does not track multiple indeces

public class PrunedQuery extends Query {


    private static final Logger LOGGER = LoggerFactory.getLogger(PrunedQuery.class);

    public PrunedQuery(final PrunedQueryEngine queryEngine) {
        super(queryEngine);
    }

    @SuppressWarnings("unchecked")
    public static void main(final String[] arg) throws Exception {

        SimpleJSAP jsap = new SimpleJSAP(PrunedQuery.class.getName(), "Loads indices relative to a collection, possibly loads the collection, and answers to queries.",
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

                        new Switch("noMplex", 'P', "noMplex", "Starts with multiplex disabled."),
                        new FlaggedOption("results", JSAP.INTEGER_PARSER, "1000", JSAP.NOT_REQUIRED, 'r', "results", "The # of results to display"),
                        new FlaggedOption("mode", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'M', "time", "The results display mode"),

                        new FlaggedOption("divert", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'd', "divert", "output file"),
                        new Switch("prune", 'e', "prune", "Enable pruned list"),
                        new Switch("globalScoring", 'G', "globalScoring", "Enable global metric scoring"),
                        new Switch("localID", 'D', "localID", "document IDs are locally numbered"),

                        new FlaggedOption("prunelist", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'L', "prunelist", "prune list"),
                        new FlaggedOption("threshold", JSAP.INTEGER_PARSER, "100", JSAP.NOT_REQUIRED, 'S', "threshold", "prune threshold percentage"),

                        new Switch("trec", 'E', "trec", "trec"),
                        new FlaggedOption("trec-runtag", JSAP.STRING_PARSER, "NYU_TANDON", JSAP.NOT_REQUIRED, 'g', "trec-runtag", "runtag for TREC output"),
                        new Switch("trecQueries", 'Q', "trecQueries", "trecQueries"),
                        new FlaggedOption("strategy", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 's', "strategy", "A serialised partitioning strategy, with local-global ID mappings"),

                        new Switch("postings", 'N', "postings", "postings"),
                        new UnflaggedOption("basenameWeight", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.GREEDY, "The indices that the servlet will use. Indices are specified using their basename, optionally followed by a colon and a double representing the weight used to score results from that index. Indices without a specified weight are weighted 1.")
                });


        final JSAPResult jsapResult = jsap.parse(arg);
        if (jsap.messagePrinted()) return;

        final DocumentCollection documentCollection = (DocumentCollection) (jsapResult.userSpecified("collection") ? AbstractDocumentSequence.load(jsapResult.getString("collection")) :
                jsapResult.userSpecified("objectCollection") ? jsapResult.getObject("objectCollection") : null);

        final String[] basenameWeight = jsapResult.getStringArray("basenameWeight");
        final Object2ReferenceLinkedOpenHashMap<String, Index> indexMap = new Object2ReferenceLinkedOpenHashMap<String, Index>(Hash.DEFAULT_INITIAL_SIZE, .5f);
        final Reference2DoubleOpenHashMap<Index> index2Weight = new Reference2DoubleOpenHashMap<Index>();
        final boolean verbose = jsapResult.getBoolean("verbose");
        final boolean loadSizes = !jsapResult.getBoolean("noSizes");
        PrunedQuery.loadIndicesFromSpec(basenameWeight, loadSizes, documentCollection, indexMap, index2Weight);

        final long numberOfDocuments = indexMap.values().iterator().next().numberOfDocuments;
        final Object2ObjectOpenHashMap<String, TermProcessor> termProcessors = new Object2ObjectOpenHashMap<String, TermProcessor>(indexMap.size());
        for (String alias : indexMap.keySet()) termProcessors.put(alias, indexMap.get(alias).termProcessor);
        final SimpleParser simpleParser = new SimpleParser(indexMap.keySet(), indexMap.firstKey(), termProcessors);
        final Reference2ReferenceMap<Index, Object> index2Parser = new Reference2ReferenceOpenHashMap<Index, Object>();

        boolean disallowScorer = false;

        final PrunedQueryEngine queryEngine = new PrunedQueryEngine(simpleParser, new DocumentIteratorBuilderVisitor(indexMap, index2Parser, indexMap.get(indexMap.firstKey()), MAX_STEMMING), indexMap);

        // use local or global metrics during scoring: can be overriden in the input query stream via $score command
        if (jsapResult.getBoolean("globalScoring")) {
            queryEngine.loadGlobalTermFrequencies(basenameWeight[0] + ".globaltermfreq");
            BM25PrunedScorer scorer = new BM25PrunedScorer(1.2, 0.3);
            scorer.setGlobalMetrics(
                    indexMap.get(indexMap.firstKey()).properties.getLong(globalPropertyKeys.G_DOCUMENTS),
                    indexMap.get(indexMap.firstKey()).properties.getLong(globalPropertyKeys.G_OCCURRENCES),
                    queryEngine.globalTermFrequencies);
            queryEngine.score(new Scorer[]{scorer, new VignaScorer()}, new double[]{1, 1});
            if (queryEngine.globalTermFrequencies.size() != indexMap.get(indexMap.firstKey()).numberOfTerms)
                throw new IllegalArgumentException("The number of global term frequencies (" + queryEngine.globalTermFrequencies.size() + " and the number of local terms do not match");
            disallowScorer = true;

            //  disable cache *** or global stat will fail
            queryEngine.equalize(0);

        } else {
            // can be overriden in the input query stream via $score command
            queryEngine.score(new Scorer[]{new BM25PrunedScorer(1.2, 0.3), new VignaScorer()}, new double[]{1, 1});
            disallowScorer = false;

            //  enable cache with lcocal stats
            queryEngine.equalize(0);
        }

        // maps between local-global IDs
        String strategyFilename = jsapResult.getString("strategy", null);
        PostingPruningStrategy strategy = (strategyFilename == null) ? null : (PostingPruningStrategy) BinIO.loadObject(strategyFilename);

        queryEngine.setWeights(index2Weight);

        // We set up an interval selector only if there is a collection for snippeting
        queryEngine.intervalSelector = documentCollection != null ? new IntervalSelector(4, 40) : new IntervalSelector();
        queryEngine.multiplex = !jsapResult.userSpecified("moPlex") || jsapResult.getBoolean("noMplex");

        // this flag is used only for document-based (emulated) pruning: uses an ordered list of documents to include and a partition threshold
        // for posting-based actual pruning, pass in a pruned index instead (built by PrunedPartition)
        queryEngine.docPrunning = jsapResult.userSpecified("prune");
        if (queryEngine.docPrunning) {
            queryEngine.loadDocumentPrunedList(jsapResult.getString("prunelist"), (int) ((jsapResult.getInt("threshold", 100) / 100.0) * (float) numberOfDocuments));
        }

        PrunedQuery query = new PrunedQuery(queryEngine);
        query.maxOutput = jsapResult.getInt("results", 1000);

        // divert output to a file?
        if (jsapResult.userSpecified("divert"))
            query.interpretCommand("$divert " + jsapResult.getObject("divert"));

        // custom TREC mode
        query.displayMode = jsapResult.userSpecified("trec") ? OutputType.TREC : OutputType.TIME;

        if (jsapResult.userSpecified("trec-runtag"))
            query.trecRunTag = jsapResult.getString("trec-runtag");

        final BigList<? extends CharSequence> titleList = (BigList<? extends CharSequence>) (
                jsapResult.userSpecified("titleList") ? BinIO.loadObject(jsapResult.getString("titleList")) :
                        jsapResult.userSpecified("titleFile") ? new FileLinesBigList(jsapResult.getString("titleFile"), "UTF-8") :
                                null);
        if (titleList != null && titleList.size64() != numberOfDocuments)
            throw new IllegalArgumentException("The number of titles (" + titleList.size64() + " and the number of documents (" + numberOfDocuments + ") do not match");

        String q;
        String[] q_parts;

        System.err.println("Welcome to the MG4J pruned query class (setup with $mode snippet, $score BM25Scorer VignaScorer, $mplex on, $equalize 1000, $select " + (documentCollection != null ? "4 40" : "all") + ")");
        if (queryEngine.docPrunning) {
            System.err.println("Running in prunning mode.");
            LOGGER.debug("Processing Query with pruned index");
        }

        System.err.println("Please type $ for help.");
        String prompt = indexMap.keySet().toString() + ">";
        int n;

        try {
            final BufferedReader br = new BufferedReader(new InputStreamReader(jsapResult.userSpecified("input") ? new FileInputStream(jsapResult.getString("input")) : System.in));
            final ObjectArrayList<DocumentScoreInfo<Reference2ObjectMap<Index, SelectedInterval[]>>> results = new ObjectArrayList<DocumentScoreInfo<Reference2ObjectMap<Index, SelectedInterval[]>>>();

            for (; ; ) {

                // trec mode
                if (query.displayMode != OutputType.TREC) System.err.print(prompt);
                q = br.readLine();
                if (q == null) {
                    System.err.println();
                    break; // CTRL-D
                }
                if (q.length() == 0) continue;
                if (q.charAt(0) == '$') {
                    if (q.contains("$score") && disallowScorer)
                        continue;
                    if (!query.interpretCommand(q)) break;
                    continue;
                }

                // if custom trec mode (topic number in query line), split out query components
                if (jsapResult.userSpecified("trec") || jsapResult.userSpecified("trecQueries")) {
                    q_parts = q.split(",");
                    if (q_parts.length != 2) {
                        System.err.println("TREC Query error *** missing topic number");
                        LOGGER.debug("TREC Query error *** missing topic number");
                        throw new IllegalArgumentException("TREC Query error *** missing topic number");
                    }
                    q = q_parts[1];
                    query.trecTopicNumber = Integer.parseInt(q_parts[0]);
                }

                // start query timing
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

                query.output(results, strategy, documentCollection, titleList, TextMarker.TEXT_BOLDFACE);

                System.err.println(results.size() + " results; " + n + " documents examined; " + time / 1000000. + " ms; " + Util.format((n * 1000000000.0) / time) + " documents/s, " + Util.format(time / (double) n) + " ns/document");
            }

        } finally {
            if (query.output != System.out) query.output.close();
        }
    }

    /**
     * Scores the given document iterator and produces score output.
     *
     * @param results            an iterator returning instances of {@link DocumentScoreInfo}.
     * @param documentCollection an optional document collection, or <code>null</code>.
     * @param titleList          an optional list of titles, or <code>null</code>.
     * @param marker             an optional text marker to mark snippets, or <code>null</code>.
     * @return the number of documents scanned.
     */
    @SuppressWarnings("boxing")
    public int output(final ObjectArrayList<DocumentScoreInfo<Reference2ObjectMap<Index, SelectedInterval[]>>> results, final PostingPruningStrategy strategy, final DocumentCollection documentCollection, final BigList<? extends CharSequence> titleList, final Marker marker) throws IOException {
        int i;
        DocumentScoreInfo<Reference2ObjectMap<Index, SelectedInterval[]>> dsi;

        if (displayMode == OutputType.TREC) {
            if (titleList == null) throw new IllegalStateException("You cannot use TREC mode without a title list");
            for (i = 0; i < results.size(); i++) {
                dsi = results.get(i);
                output.println(trecTopicNumber + " Q0 " + titleList.get(dsi.document) + " " + i + " " + FORMATTER.format(dsi.score) + " " + trecRunTag);
            }

            // Horrible patch for no-answer queries (a workaround for TREC necessity of at least one result per query)
            if (results.size() == 0) output.println(trecTopicNumber + " Q0 GX000-00-0000000 1 0 " + trecRunTag);
        } else {

            for (i = 0; i < results.size(); i++) {
                dsi = results.get(i);

                // check whether this is a pruned index and we need global IDs
                final long document = dsi.document;
                if (strategy == null) {
                    output.print(document);
                } else {
                    long globalID = strategy.globalPointer(0, document);
                    output.print(globalID);
                }

                Document d = null; // Filled lazily

                // We try to print a title, preferring the supplied title list if present
                if (titleList != null) output.println(" " + titleList.get(document));
                else if (documentCollection != null) {
                    d = documentCollection.document(document);
                    output.println(" " + d.title().toString().trim());
                } else output.println();

                if ((displayMode == OutputType.LONG || displayMode == OutputType.SNIPPET) && dsi.info != null && queryEngine.intervalSelector != null) {
                    final Index[] sortedIndex = dsi.info.keySet().toArray(new Index[0]);
                    if (documentCollection != null) Arrays.sort(sortedIndex, new Comparator<Index>() {
                        public int compare(final Index i0, final Index i1) {
                            return documentCollection.factory().fieldIndex(i0.field) - documentCollection.factory().fieldIndex(i1.field);
                        }
                    });
                    for (Index index : sortedIndex)
                        if (index.hasPositions) {
                            SelectedInterval[] interval = dsi.info.get(index);
                            if (interval == SelectedInterval.TRUE_ARRAY) output.println(index.field + ": TRUE");
                            else if (interval == SelectedInterval.FALSE_ARRAY) output.println(index.field + ": FALSE");
                            else if (displayMode == OutputType.LONG || documentCollection == null)
                                output.println(index.field + ": " + Arrays.toString(interval));
                            else { // SNIPPET_MODE
                                final MarkingMutableString s = new MarkingMutableString(marker);
                                s.startField(interval);
                                // TODO: this must be in increasing field order
                                if (d == null) d = documentCollection.document(document);
                                int fieldIndex = documentCollection.factory().fieldIndex(index.field);
                                if (fieldIndex == -1 || documentCollection.factory().fieldType(fieldIndex) != DocumentFactory.FieldType.TEXT)
                                    continue;
                                final Reader reader = (Reader) d.content(fieldIndex);
                                s.appendAndMark(d.wordReader(fieldIndex).setReader(reader));
                                s.endField();
                                output.println(index.field + ": " + s.toString());
                            }
                        } else if (index.hasPayloads && dsi.info.get(index) == SelectedInterval.TRUE_ARRAY) {
                            if (d == null) d = documentCollection.document(document);
                            int fieldIndex = documentCollection.factory().fieldIndex(index.field);
                            if (fieldIndex == -1) continue;
                            output.println(d.content(fieldIndex));
                        }
                    output.println();
                }

                if (d != null) d.close();
            }
        }
        if (displayMode != OutputType.TREC) output.println();
        return i;
    }

    /**
     * Symbolic names for global metrics
     */
    public enum globalPropertyKeys {
        /**
         * The number of documents in the collection.
         */
        G_DOCUMENTS,
        /**
         * The number of terms in the collection.
         */
        G_TERMS,
        /**
         * The number of occurrences in the collection, or -1 if the number of occurrences is not known.
         */
        G_OCCURRENCES,
        /**
         * The number of postings (pairs term/document) in the collection.
         */
        G_POSTINGS,
        /**
         * The number of batches this index was (or should be) built from.
         */
        G_MAXCOUNT,
        /**
         * The maximum size (in words) of a document, or -1 if the maximum document size is not known.
         */
        G_MAXDOCSIZE
    }
}
