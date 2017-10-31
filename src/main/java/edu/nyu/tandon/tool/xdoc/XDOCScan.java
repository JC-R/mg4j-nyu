package edu.nyu.tandon.tool.xdoc;


import com.martiansoftware.jsap.*;
import edu.nyu.tandon.utils.languageModel;
import it.unimi.di.big.mg4j.document.*;
import it.unimi.di.big.mg4j.index.DowncaseTermProcessor;
import it.unimi.di.big.mg4j.index.NullTermProcessor;
import it.unimi.di.big.mg4j.index.TermProcessor;
import it.unimi.di.big.mg4j.index.payload.DatePayload;
import it.unimi.di.big.mg4j.index.payload.IntegerPayload;
import it.unimi.di.big.mg4j.io.ByteArrayPostingList;
import it.unimi.di.big.mg4j.io.IOFactories;
import it.unimi.di.big.mg4j.io.IOFactory;
import it.unimi.di.big.mg4j.tool.Scan;
import it.unimi.di.big.mg4j.tool.VirtualDocumentResolver;
import it.unimi.di.big.mg4j.util.MG4JClassParser;
import it.unimi.dsi.Util;
import it.unimi.dsi.fastutil.ints.IntBigArrays;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.io.FastBufferedReader;
import it.unimi.dsi.io.OutputBitStream;
import it.unimi.dsi.io.WordReader;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.lang.ObjectParser;
import it.unimi.dsi.logging.ProgressLogger;
import org.apache.commons.configuration.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.io.Reader;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.sun.jna.Native.getNativeSize;

/**
 * Created by Juan on 7/20/2015.
 */
public class XDOCScan extends Scan {

    protected final static Logger LOGGER = LoggerFactory.getLogger(XDOCScan.class);
    protected static final char[] LINE_TERMINATORS = new char[]{'\n', '\r'};
    protected static final char[] SPACES = new char[]{' ', ' '};
    public static OutputBitStream docIndex;
    protected static String UnigramName = "model.arpa";
    private static String XDOC_basename;
    private static int XDOC_batch;
    private static File XDOC_batchDir;
    public languageModel lm;

    /**
     * Creates a new Scanner w/XDOC logic.
     */
    @Deprecated
    public XDOCScan(final String lmName, final String basename, final String field, final TermProcessor termProcessor, final IndexingType indexingType,
                    final int bufferSize, final DocumentCollectionBuilder builder, final File batchDir) throws IOException {
        super(basename, field, it.unimi.di.big.mg4j.tool.Scan.Completeness.POSITIONS, termProcessor, indexingType, 0, 0, bufferSize, builder, batchDir);
        localInit(lmName);
    }

    public XDOCScan(final String lmName, final String basename, final String field, final it.unimi.di.big.mg4j.tool.Scan.Completeness completeness,
                    final TermProcessor termProcessor, final IndexingType indexingType, final int numVirtualDocs, final int virtualDocumentGap,
                    final int bufferSize, final DocumentCollectionBuilder builder, final File batchDir) throws IOException {
        super(IOFactory.FILESYSTEM_FACTORY, basename, field, completeness, termProcessor, indexingType, numVirtualDocs, virtualDocumentGap, bufferSize, builder, batchDir);
        localInit(lmName);
    }

    public XDOCScan(final IOFactory ioFactory, final String lmName, final String basename, final String field, final it.unimi.di.big.mg4j.tool.Scan.Completeness completeness,
                    final TermProcessor termProcessor, final IndexingType indexingType, final long numVirtualDocs, final int virtualDocumentGap,
                    final int bufferSize, final DocumentCollectionBuilder builder, final File batchDir) throws IOException {
        super(ioFactory, basename, field, completeness, termProcessor, indexingType, numVirtualDocs, virtualDocumentGap, bufferSize, builder, batchDir);
        localInit(lmName);
    }

    private static void dumpXDOCbatch(final IOFactory ioFactory) throws IOException {
        docIndex.close();
        docIndex = new OutputBitStream(ioFactory.getOutputStream(batchBasename(++XDOC_batch, XDOC_basename, XDOC_batchDir) + ".xdoc"), false);
    }

    public static void main(final String[] arg) throws JSAPException, InvocationTargetException, NoSuchMethodException, ConfigurationException, ClassNotFoundException, IOException,
            IllegalAccessException, InstantiationException {


        SimpleJSAP jsap = new SimpleJSAP(
                Scan.class.getName(),
                "Builds a set of batches from a sequence of documents.",
                new Parameter[]{
                        new FlaggedOption("sequence", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'S', "sequence", "A serialised document sequence that will be used instead of stdin."),
                        new FlaggedOption("ioFactory", JSAP.STRING_PARSER, "FILESYSTEM_FACTORY", JSAP.NOT_REQUIRED, JSAP.NO_SHORTFLAG, "io-factory", "An I/O factory that will be used to create files (either a static field of IOFactory or an object specification)."),
                        new FlaggedOption("objectSequence", new ObjectParser(DocumentSequence.class, MG4JClassParser.PACKAGE), JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'o', "object-sequence", "An object specification describing a document sequence that will be used instead of stdin."),
                        new FlaggedOption("delimiter", JSAP.INTEGER_PARSER, Integer.toString(DEFAULT_DELIMITER), JSAP.NOT_REQUIRED, 'd', "delimiter", "The document delimiter (when indexing stdin)."),
                        new FlaggedOption("factory", MG4JClassParser.getParser(), IdentityDocumentFactory.class.getName(), JSAP.NOT_REQUIRED, 'f', "factory", "A document factory with a standard constructor (when indexing stdin)."),
                        new FlaggedOption("property", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'p', "property", "A 'key=value' specification, or the name of a property file (when indexing stdin).")
                                .setAllowMultipleDeclarations(true),
                        new FlaggedOption("termProcessor", JSAP.STRING_PARSER, NullTermProcessor.class.getName(), JSAP.NOT_REQUIRED, 't', "term-processor",
                                "Sets the term processor to the given class."),
                        new FlaggedOption("completeness", JSAP.STRING_PARSER, Completeness.POSITIONS.name(), JSAP.NOT_REQUIRED, 'c', "completeness", "How complete the index should be " + Arrays.toString(Completeness.values()) + "."),
                        new Switch("downcase", JSAP.NO_SHORTFLAG, "downcase", "A shortcut for setting the term processor to the downcasing processor."),
                        new FlaggedOption("indexedField", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'I', "indexed-field",
                                "The field(s) of the document factory that will be indexed. (default: all non-virtual fields)").setAllowMultipleDeclarations(true),
                        new Switch("allFields", 'a', "all-fields", "Index also all virtual fields; has no effect if indexedField has been used at least once."),
                        new FlaggedOption("buildCollection", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'B', "build-collection", "During the indexing phase, build a collection using this basename."),
                        new FlaggedOption("builderClass", MG4JClassParser.getParser(), SimpleCompressedDocumentCollectionBuilder.class.getName(), JSAP.NOT_REQUIRED, JSAP.NO_SHORTFLAG, "builder-class", "Specifies a builder class for a document collection that will be created during the indexing phase."),
                        new Switch("exact", 'e', "exact", "The builder class should be instantiated in its exact form, which records both words and nonwords."),
                        new FlaggedOption("batchSize", JSAP.INTSIZE_PARSER, Integer.toString(Scan.DEFAULT_BATCH_SIZE), JSAP.NOT_REQUIRED, 's', "batch-size", "The maximum size of a batch, in documents. Batches will be smaller, however, if memory is exhausted or there are too many terms."),
                        new FlaggedOption("maxTerms", JSAP.INTSIZE_PARSER, Integer.toString(Scan.DEFAULT_MAX_TERMS), JSAP.NOT_REQUIRED, 'M', "max-terms", "The maximum number of terms in a batch, in documents."),
                        new FlaggedOption("virtualDocumentResolver", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'v', "virtual-document-resolver",
                                "The virtual document resolver. It can be specified several times in the form [<field>:]<filename>. If the field is omitted, it sets the document resolver for all virtual fields.")
                                .setAllowMultipleDeclarations(true),
                        new FlaggedOption("virtualDocumentGap", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'g', "virtual-document-gap",
                                "The virtual document gap. It can be specified several times in the form [<field>:]<gap>. If the field is omitted, it sets the document gap for all virtual fields; the default gap is "
                                        + DEFAULT_VIRTUAL_DOCUMENT_GAP).setAllowMultipleDeclarations(true),
                        new FlaggedOption("bufferSize", JSAP.INTSIZE_PARSER, Util.formatBinarySize(DEFAULT_BUFFER_SIZE), JSAP.NOT_REQUIRED, 'b', "buffer-size", "The size of an I/O buffer."),
                        new FlaggedOption("renumber", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'r', "renumber", "The filename of a document renumbering."),
                        new Switch("keepUnsorted", 'u', "keep-unsorted", "Keep the unsorted term file."),
                        new FlaggedOption("logInterval", JSAP.LONG_PARSER, Long.toString(ProgressLogger.DEFAULT_LOG_INTERVAL), JSAP.NOT_REQUIRED, 'l', "log-interval",
                                "The minimum time interval between activity logs in milliseconds."),
                        new FlaggedOption("tempDir", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'T', "temp-dir", "A directory for all temporary files (e.g., batches)."),
                        new FlaggedOption("language-model", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NO_SHORTFLAG, "language-model", "An ARPA file to be used to create a unigram model."),
                        new UnflaggedOption("basename", JSAP.STRING_PARSER, JSAP.REQUIRED, "The basename of the resulting index.")
                });

        JSAPResult jsapResult = jsap.parse(arg);
        if (jsap.messagePrinted()) return;

        if ((jsapResult.userSpecified("builderClass") || jsapResult.userSpecified("exact")) && !jsapResult.userSpecified("buildCollection"))
            throw new IllegalArgumentException("To specify options about the collection building process, you must specify a basename first.");
        if (jsapResult.userSpecified("sequence") && jsapResult.userSpecified("objectSequence"))
            throw new IllegalArgumentException("You cannot specify both a serialised and an parseable-object sequence");

        final DocumentSequence documentSequence = jsapResult.userSpecified("objectSequence") ? (DocumentSequence) jsapResult.getObject("objectSequence") : Scan.getSequence(jsapResult.getString("sequence"), jsapResult.getClass("factory"), jsapResult.getStringArray("property"), jsapResult.getInt("delimiter"), LOGGER);

        final IOFactory ioFactory = parseIOFactory(jsapResult.getString("ioFactory"));

        final DocumentFactory factory = documentSequence.factory();
        final int[] indexedField = parseFieldNames(jsapResult.getStringArray("indexedField"), factory, jsapResult.getBoolean("allFields"));
        final int batchSize = jsapResult.getInt("batchSize");
        final VirtualDocumentResolver[] virtualDocumentResolver = parseVirtualDocumentResolver(ioFactory, jsapResult.getStringArray("virtualDocumentResolver"), indexedField, factory);
        final int[] virtualDocumentGap = parseVirtualDocumentGap(jsapResult.getStringArray("virtualDocumentGap"), indexedField, factory);

        DocumentCollectionBuilder builder = null;
        if (jsapResult.userSpecified("buildCollection")) {
            final Class<? extends DocumentCollectionBuilder> builderClass = jsapResult.getClass("builderClass");
            try {
                // Try first IOFactory-based constructor.
                builder = builderClass != null ? builderClass.getConstructor(IOFactory.class, String.class, DocumentFactory.class, boolean.class).newInstance(
                        ioFactory, jsapResult.getString("buildCollection"),
                        documentSequence.factory().numberOfFields() == indexedField.length ? documentSequence.factory().copy() : new SubDocumentFactory(documentSequence.factory().copy(), indexedField),
                        jsapResult.getBoolean("exact")) : null;
            } catch (NoSuchMethodException noIOFactoryConstructor) {
                builder = builderClass != null ? builderClass.getConstructor(String.class, DocumentFactory.class, boolean.class).newInstance(
                        jsapResult.getString("buildCollection"),
                        documentSequence.factory().numberOfFields() == indexedField.length ? documentSequence.factory().copy() : new SubDocumentFactory(documentSequence.factory().copy(), indexedField),
                        jsapResult.getBoolean("exact")) : null;
                if (builder != null)
                    LOGGER.warn("The builder class " + builderClass.getName() + " has no IOFactory-based constructor");
            }
        }

        run(ioFactory, jsapResult.getString("basename"), documentSequence, Completeness.valueOf(jsapResult.getString("completeness")), jsapResult.getBoolean("downcase") ? DowncaseTermProcessor.getInstance() : ObjectParser.fromSpec(jsapResult
                .getString("termProcessor"), TermProcessor.class, MG4JClassParser.PACKAGE, new String[]{"getInstance"}), builder, jsapResult
                .getInt("bufferSize"), batchSize, jsapResult.getInt("maxTerms"), indexedField, virtualDocumentResolver, virtualDocumentGap, jsapResult.getString("renumber"), jsapResult.getLong("logInterval"), jsapResult
                .getString("tempDir"), jsapResult.getString("language-model"));
    }

    public static void run(final IOFactory ioFactory, final String basename, final DocumentSequence documentSequence, final Completeness completeness, final TermProcessor termProcessor, final DocumentCollectionBuilder builder, final int bufferSize,
                           final int documentsPerBatch, final int maxTerms, final int[] indexedField, final VirtualDocumentResolver[] virtualDocumentResolver, final int[] virtualGap, final String mapFile, final long logInterval,
                           final String tempDirName, final String lmName) throws ConfigurationException, IOException {

        final boolean building = builder != null;
        final int numberOfIndexedFields = indexedField.length;
        if (numberOfIndexedFields == 0) throw new IllegalArgumentException("You must specify at least one field");
        final DocumentFactory factory = documentSequence.factory();
        final File tempDir = tempDirName == null ? null : new File(tempDirName);
        for (int i = 0; i < indexedField.length; i++)
            if (factory.fieldType(indexedField[i]) == DocumentFactory.FieldType.VIRTUAL && (virtualDocumentResolver == null || virtualDocumentResolver[i] == null))
                throw new IllegalArgumentException(
                        "No resolver was associated with virtual field " + factory.fieldName(indexedField[i]));

        if (mapFile != null && ioFactory != IOFactory.FILESYSTEM_FACTORY)
            throw new IllegalStateException("Remapped indices currently do not support I/O factories");
        final int[] map = mapFile != null ? BinIO.loadInts(mapFile) : null;

        final Scan[] scan = new XDOCScan[numberOfIndexedFields]; // To scan textual content
        final PayloadAccumulator[] accumulator = new PayloadAccumulator[numberOfIndexedFields]; // To accumulate
        // document data

        final ProgressLogger pl = new ProgressLogger(LOGGER, logInterval, TimeUnit.MILLISECONDS, "documents");
        if (documentSequence instanceof DocumentCollection)
            pl.expectedUpdates = ((DocumentCollection) documentSequence).size();

        final PrintStream titleStream = new PrintStream(basename + Scan.TITLES_EXTENSION, "UTF-8");

        for (int i = 0; i < numberOfIndexedFields; i++) {
            final String fieldName = factory.fieldName(indexedField[i]);
            switch (factory.fieldType(indexedField[i])) {
                case TEXT:
                    scan[i] = new XDOCScan(ioFactory, lmName, basename + '-' + fieldName, fieldName, completeness, termProcessor, map != null ? IndexingType.REMAPPED
                            : IndexingType.STANDARD, 0, 0, bufferSize, builder, tempDir);
                    break;
                case VIRTUAL:
                    scan[i] = new XDOCScan(ioFactory, lmName, basename + '-' + fieldName, fieldName, completeness, termProcessor, IndexingType.VIRTUAL,
                            virtualDocumentResolver[i].numberOfDocuments(), virtualGap[i], bufferSize, builder, tempDir);
                    break;

                case DATE:
                    accumulator[i] = new PayloadAccumulator(ioFactory, basename + '-' + fieldName, new DatePayload(), fieldName,
                            map != null ? IndexingType.REMAPPED : IndexingType.STANDARD, documentsPerBatch, tempDir);
                    break;
                case INT:
                    accumulator[i] = new PayloadAccumulator(ioFactory, basename + '-' + fieldName, new IntegerPayload(), fieldName,
                            map != null ? IndexingType.REMAPPED : IndexingType.STANDARD, documentsPerBatch, tempDir);
                    break;
                default:

            }
        }

        if (building) builder.open("@0"); // First batch

        pl.displayFreeMemory = true;
        pl.start("Indexing documents...");

        DocumentIterator iterator = documentSequence.iterator();
        Reader reader;
        WordReader wordReader;
        List<VirtualDocumentFragment> fragments;
        Document document;

        int documentPointer = 0, documentsInBatch = 0;
        long batchStartTime = System.currentTimeMillis();
        boolean outOfMemoryError = false;
        final MutableString title = new MutableString();

        while ((document = iterator.nextDocument()) != null) {

            long overallTerms = 0;
            if (document.title() != null) {
                title.replace(document.title());
                title.replace(LINE_TERMINATORS, SPACES);
                titleStream.print(title);
            }
            titleStream.println();
            if (building) builder.startDocument(document.title(), document.uri());
            for (int i = 0; i < numberOfIndexedFields; i++) {
                switch (factory.fieldType(indexedField[i])) {
                    case TEXT:
                        reader = (Reader) document.content(indexedField[i]);
                        wordReader = document.wordReader(indexedField[i]);
                        wordReader.setReader(reader);
                        if (building) builder.startTextField();
                        scan[i].processDocument(map != null ? map[documentPointer] : documentPointer, wordReader);
                        if (building) builder.endTextField();
                        overallTerms += scan[i].numTerms;
                        break;
                    case VIRTUAL:
                        fragments = (List<VirtualDocumentFragment>) document.content(indexedField[i]);
                        wordReader = document.wordReader(indexedField[i]);
                        virtualDocumentResolver[i].context(document);
                        for (VirtualDocumentFragment fragment : fragments) {
                            long virtualDocumentPointer = virtualDocumentResolver[i].resolve(fragment.documentSpecifier());
                            if (virtualDocumentPointer < 0) continue;
                            // ALERT: we must rewrite remapping to work with long-sized document pointers.
                            if (map != null) virtualDocumentPointer = map[(int) virtualDocumentPointer];
                            wordReader.setReader(new FastBufferedReader(fragment.text()));
                            scan[i].processDocument((int) virtualDocumentPointer, wordReader);
                        }
                        if (building) builder.virtualField(fragments);
                        overallTerms += scan[i].numTerms;
                        break;
                    default:
                        Object o = document.content(indexedField[i]);
                        accumulator[i].processData(map != null ? map[documentPointer] : documentPointer, o);
                        if (building) builder.nonTextField(o);
                        break;
                }

                if (scan[i] != null && scan[i].outOfMemoryError) outOfMemoryError = true;
            }
            if (building) builder.endDocument();
            documentPointer++;
            documentsInBatch++;
            document.close();
            pl.update();

            long percAvailableMemory = 100;
            boolean compacted = false;
            if ((documentPointer & 0xFF) == 0) {
                // We try compaction if we detect less than PERC_AVAILABLE_MEMORY_CHECK memory available
                percAvailableMemory = Util.percAvailableMemory();
                if (!outOfMemoryError && percAvailableMemory < PERC_AVAILABLE_MEMORY_CHECK) {
                    LOGGER.info("Starting compaction... (" + percAvailableMemory + "% available)");
                    compacted = true;
                    Util.compactMemory();
                    percAvailableMemory = Util.percAvailableMemory();
                    LOGGER.info("Compaction completed (" + percAvailableMemory + "% available)");
                }
            }

            if (outOfMemoryError || overallTerms >= maxTerms || documentsInBatch == documentsPerBatch || (compacted && percAvailableMemory < PERC_AVAILABLE_MEMORY_DUMP)) {
                if (outOfMemoryError)
                    LOGGER.warn("OutOfMemoryError during buffer reallocation: writing a batch of " + documentsInBatch + " documents");
                else if (overallTerms >= maxTerms)
                    LOGGER.warn("Too many terms (" + overallTerms + "): writing a batch of " + documentsInBatch + " documents");
                else if (compacted && percAvailableMemory < PERC_AVAILABLE_MEMORY_DUMP)
                    LOGGER.warn("Available memory below " + PERC_AVAILABLE_MEMORY_DUMP + "%: writing a batch of " + documentsInBatch + " documents");

                long occurrences = 0;
                for (int i = 0; i < numberOfIndexedFields; i++) {
                    switch (factory.fieldType(indexedField[i])) {
                        case TEXT:
                        case VIRTUAL:
                            occurrences += scan[i].dumpBatch();
                            scan[i].openSizeBitStream();
                            break;
                        default:
                            accumulator[i].writeData();
                    }
                }

                // dump this XDCOC batch
                dumpXDOCbatch(ioFactory);

                if (building) {
                    builder.close();
                    builder.open("@" + scan[0].batch);
                }

                LOGGER.info("Last set of batches indexed at " + Util.format((1000. * occurrences) / (System.currentTimeMillis() - batchStartTime)) + " occurrences/s");
                batchStartTime = System.currentTimeMillis();
                documentsInBatch = 0;
                outOfMemoryError = false;
            }
        }

        iterator.close();
        titleStream.close();
        if (builder != null) builder.close();

        for (int i = 0; i < numberOfIndexedFields; i++) {
            switch (factory.fieldType(indexedField[i])) {
                case TEXT:
                case VIRTUAL:
                    scan[i].close();
                    docIndex.close();
//                    Corpus.close();
                    break;
                default:
                    accumulator[i].close();
                    break;
            }
        }

        documentSequence.close();

        pl.done();

        if (building) {
            final String name = new File(builder.basename()).getName();
            final String[] collectionName = new String[scan[0].batch];
            for (int i = scan[0].batch; i-- != 0; )
                collectionName[i] = name + "@" + i + DocumentCollection.DEFAULT_EXTENSION;
            IOFactories.storeObject(ioFactory, new ConcatenatedDocumentCollection(collectionName), builder.basename() + DocumentCollection.DEFAULT_EXTENSION);
        }

        if (map != null && documentPointer != map.length)
            LOGGER.warn("The document sequence contains " + documentPointer + " documents, but the map contains "
                    + map.length + " integers");
    }

    private void localInit(String fileName) throws IOException {
        try {
            LOGGER.info("Reading ARPA file: " + fileName);
            lm = new languageModel(fileName);
            LOGGER.info("ARPA file: " + Util.format(lm.Unigrams.size()) + " unigrams");
        } catch (Exception e) {
            throw new IllegalArgumentException("Unigrams file");
        }
        XDOC_basename = basename;
        XDOC_batchDir = batchDir;
        XDOC_batch = batch;
        docIndex = new OutputBitStream(ioFactory.getOutputStream(batchBasename(batch, basename, batchDir) + ".xdoc"), false);
    }

    /**
     * Processes a document.
     *
     * @param documentPointer the integer pointer associated with the document.
     * @param wordReader      the word reader associated with the document.
     */
    @Override
    public void processDocument(final long documentPointer, final WordReader wordReader) throws IOException {

        int pos = indexingIsVirtual ? IntBigArrays.get(currSize, documentPointer) : 0;
        final long actualPointer = indexingIsStandard ? documentCount : documentPointer;
        ByteArrayPostingList termBapl;

        // local doc unique terms
        HashSet<MutableString> termList = new HashSet<MutableString>();
        float xdoc = 0;
        int docTerms = 0;

        final int floatLen = getNativeSize(float.class);

        word.length(0);
        nonWord.length(0);

        while (wordReader.next(word, nonWord)) {

            if (builder != null) builder.add(word, nonWord);
            if (word.length() == 0) continue;
            if (!termProcessor.processTerm(word)) {
                pos++; // We do consider the positions of terms canceled out by the term processor.
                continue;
            }

            if (termList.add(word)) {
                xdoc += lm.Unigrams.getFloat(word); // compute XDOC
                docTerms++;
            }

            // We check whether we have already seen this term. If not, we add it to the term map.
            if ((termBapl = termMap.get(word)) == null) {
                try {
                    termBapl = new ByteArrayPostingList(new byte[BYTE_ARRAY_POSTING_LIST_INITIAL_SIZE], indexingIsStandard, completeness);
                    termMap.put(word.copy(), termBapl);
                } catch (OutOfMemoryError e) {
                    /* There is not enough memory for enlarging the table. We set a very low growth factor, so at
                     * the next put() the enlargement will likely succeed. If not, we will generate several
					 * out-of-memory error, but we should get to the end anyway, and we will
					 * dump the current batch as soon as the current document is finished. */
                    outOfMemoryError = true;
                    //termMap.growthFactor( 1 );
                }
                numTerms++;
                if (numTerms % TERM_REPORT_STEP == 0) LOGGER.info("[" + Util.format(numTerms) + " term(s)]");
            }

            // We now record the occurrence. If a renumbering map has
            // been specified, we have to renumber the document index through it.
            termBapl.setDocumentPointer(actualPointer);
            termBapl.addPosition(pos);

            // Record whether this posting list has an out-of-memory-error problem.
            if (termBapl.outOfMemoryError) outOfMemoryError = true;
            occsInCurrDoc++;
            numOccurrences++;
            pos++;
        }

        if (pos > maxDocSize) maxDocSize = pos;

        if (indexingIsStandard) {
            sizes.writeGamma(pos);
        } else if (indexingIsRemapped) {
            sizes.writeLongGamma(actualPointer);
            sizes.writeGamma(pos);
        }

        docIndex.writeLongGamma(actualPointer);
        docIndex.writeGamma(pos);
        docIndex.writeGamma(docTerms);
        docIndex.writeInt(Float.floatToIntBits(xdoc), 32);

        if (indexingIsVirtual)
            IntBigArrays.set(currSize, documentPointer, IntBigArrays.get(currSize, documentPointer) + occsInCurrDoc + virtualDocumentGap);

        pos = occsInCurrDoc = 0;
        documentCount++;
        if (actualPointer > maxDocInBatch) maxDocInBatch = actualPointer;

    }


}
