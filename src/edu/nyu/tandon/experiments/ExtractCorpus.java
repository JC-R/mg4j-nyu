package edu.nyu.tandon.experiments;


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

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.sun.jna.Native.getNativeSize;

/**
 * Created by Juan on 7/20/2015.
 */
public class ExtractCorpus extends Scan {


    protected static PrintStream Corpus;

    protected final static Logger LOGGER = LoggerFactory.getLogger(Scan.class);
    protected static final char[] LINE_TERMINATORS = new char[]{'\n', '\r'};
    protected static final char[] SPACES = new char[]{' ', ' '};

    private void localInit() throws IOException {
        Corpus = new PrintStream( basename + "corpus", "UTF-8" );
    }

    /**
     * Creates a new Scanner w/XDOC logic.
     */
    @Deprecated
    public ExtractCorpus(final String basename, final String field, final TermProcessor termProcessor, final IndexingType indexingType,
                         final int bufferSize, final DocumentCollectionBuilder builder, final File batchDir) throws IOException {
        super(basename, field, Completeness.POSITIONS, termProcessor, indexingType, 0, 0, bufferSize, builder, batchDir);
        localInit();
    }

    public ExtractCorpus(final String basename, final String field, final Completeness completeness,
                         final TermProcessor termProcessor, final IndexingType indexingType, final int numVirtualDocs, final int virtualDocumentGap,
                         final int bufferSize, final DocumentCollectionBuilder builder, final File batchDir) throws IOException {
        super(IOFactory.FILESYSTEM_FACTORY, basename, field, completeness, termProcessor, indexingType, numVirtualDocs, virtualDocumentGap, bufferSize, builder, batchDir);
        localInit();
    }

    public ExtractCorpus(final IOFactory ioFactory, final String basename, final String field, final Completeness completeness,
                         final TermProcessor termProcessor, final IndexingType indexingType, final long numVirtualDocs, final int virtualDocumentGap,
                         final int bufferSize, final DocumentCollectionBuilder builder, final File batchDir) throws IOException {
        super(ioFactory, basename, field, completeness, termProcessor, indexingType, numVirtualDocs, virtualDocumentGap, bufferSize, builder, batchDir);
        localInit();
    }

    /**
     * Processes a document.
     *
     * @param documentPointer the integer pointer associated with the document.
     * @param wordReader      the word reader associated with the document.
     */
    @Override
    public void processDocument(final long documentPointer, final WordReader wordReader) throws IOException {
        word.length(0);
        nonWord.length(0);
        while (wordReader.next(word, nonWord)) {
            if (word.length() == 0) continue;
            Corpus.print(word.toString()+ " ");
            continue;
        }
        Corpus.println();
    }

    public static void main(final String[] arg) throws JSAPException, InvocationTargetException, NoSuchMethodException, ConfigurationException, ClassNotFoundException, IOException,
            IllegalAccessException, InstantiationException {

        SimpleJSAP jsap = new SimpleJSAP(
                Scan.class.getName(),
                "Builds a set of batches from a sequence of documents.",
                new Parameter[] {
                        new FlaggedOption( "sequence", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'S', "sequence", "A serialised document sequence that will be used instead of stdin." ),
                        new FlaggedOption( "ioFactory", JSAP.STRING_PARSER, "FILESYSTEM_FACTORY", JSAP.NOT_REQUIRED, JSAP.NO_SHORTFLAG, "io-factory", "An I/O factory that will be used to create files (either a static field of IOFactory or an object specification)." ),
                        new FlaggedOption( "objectSequence", new ObjectParser( DocumentSequence.class, MG4JClassParser.PACKAGE ), JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'o', "object-sequence", "An object specification describing a document sequence that will be used instead of stdin." ),
                        new FlaggedOption( "delimiter", JSAP.INTEGER_PARSER, Integer.toString( DEFAULT_DELIMITER ), JSAP.NOT_REQUIRED, 'd', "delimiter", "The document delimiter (when indexing stdin)." ),
                        new FlaggedOption( "factory", MG4JClassParser.getParser(), IdentityDocumentFactory.class.getName(), JSAP.NOT_REQUIRED, 'f', "factory", "A document factory with a standard constructor (when indexing stdin)." ),
                        new FlaggedOption( "property", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'p', "property", "A 'key=value' specification, or the name of a property file (when indexing stdin)." )
                                .setAllowMultipleDeclarations( true ),
                        new FlaggedOption( "termProcessor", JSAP.STRING_PARSER, NullTermProcessor.class.getName(), JSAP.NOT_REQUIRED, 't', "term-processor",
                                "Sets the term processor to the given class." ),
                        new FlaggedOption( "completeness", JSAP.STRING_PARSER, Completeness.POSITIONS.name(), JSAP.NOT_REQUIRED, 'c', "completeness", "How complete the index should be " + Arrays.toString( Completeness.values() ) + "." ),
                        new Switch( "downcase", JSAP.NO_SHORTFLAG, "downcase", "A shortcut for setting the term processor to the downcasing processor." ),
                        new FlaggedOption( "indexedField", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'I', "indexed-field",
                                "The field(s) of the document factory that will be indexed. (default: all non-virtual fields)" ).setAllowMultipleDeclarations( true ),
                        new Switch( "allFields", 'a', "all-fields", "Index also all virtual fields; has no effect if indexedField has been used at least once." ),
                        new Switch( "exact", 'e', "exact", "The builder class should be instantiated in its exact form, which records both words and nonwords." ),
                        new FlaggedOption( "batchSize", JSAP.INTSIZE_PARSER, Integer.toString( Scan.DEFAULT_BATCH_SIZE ), JSAP.NOT_REQUIRED, 's', "batch-size", "The maximum size of a batch, in documents. Batches will be smaller, however, if memory is exhausted or there are too many terms." ),
                        new FlaggedOption( "maxTerms", JSAP.INTSIZE_PARSER, Integer.toString( Scan.DEFAULT_MAX_TERMS ), JSAP.NOT_REQUIRED, 'M', "max-terms", "The maximum number of terms in a batch, in documents." ),
                        new FlaggedOption( "virtualDocumentResolver", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'v', "virtual-document-resolver",
                                "The virtual document resolver. It can be specified several times in the form [<field>:]<filename>. If the field is omitted, it sets the document resolver for all virtual fields." )
                                .setAllowMultipleDeclarations( true ),
                        new FlaggedOption( "virtualDocumentGap", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'g', "virtual-document-gap",
                                "The virtual document gap. It can be specified several times in the form [<field>:]<gap>. If the field is omitted, it sets the document gap for all virtual fields; the default gap is "
                                        + DEFAULT_VIRTUAL_DOCUMENT_GAP ).setAllowMultipleDeclarations( true ),
                        new FlaggedOption( "bufferSize", JSAP.INTSIZE_PARSER, Util.formatBinarySize( DEFAULT_BUFFER_SIZE ), JSAP.NOT_REQUIRED, 'b', "buffer-size", "The size of an I/O buffer." ),
                        new FlaggedOption( "renumber", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'r', "renumber", "The filename of a document renumbering." ),
                        new Switch( "keepUnsorted", 'u', "keep-unsorted", "Keep the unsorted term file." ),
                        new FlaggedOption( "logInterval", JSAP.LONG_PARSER, Long.toString( ProgressLogger.DEFAULT_LOG_INTERVAL ), JSAP.NOT_REQUIRED, 'l', "log-interval",
                                "The minimum time interval between activity logs in milliseconds." ),
                        new FlaggedOption( "tempDir", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'T', "temp-dir", "A directory for all temporary files (e.g., batches)." ),
                        new UnflaggedOption( "basename", JSAP.STRING_PARSER, JSAP.REQUIRED, "The basename of the resulting index." )

                } );

        JSAPResult jsapResult = jsap.parse( arg );
        if ( jsap.messagePrinted() ) return;

        if ( jsapResult.userSpecified( "sequence" ) && jsapResult.userSpecified( "objectSequence" ) ) throw new IllegalArgumentException( "You cannot specify both a serialised and an parseable-object sequence" );

        final DocumentSequence documentSequence = jsapResult.userSpecified( "objectSequence" ) ? (DocumentSequence)jsapResult.getObject( "objectSequence" ) : Scan.getSequence( jsapResult.getString( "sequence" ), jsapResult.getClass( "factory" ), jsapResult.getStringArray( "property" ), jsapResult.getInt( "delimiter" ), LOGGER );

        final IOFactory ioFactory = parseIOFactory( jsapResult.getString( "ioFactory" ) );

        final DocumentFactory factory = documentSequence.factory();
        final int[] indexedField = parseFieldNames( jsapResult.getStringArray( "indexedField" ), factory, jsapResult.getBoolean( "allFields" ) );
        final int batchSize = jsapResult.getInt( "batchSize" );
        final VirtualDocumentResolver[] virtualDocumentResolver = parseVirtualDocumentResolver( ioFactory, jsapResult.getStringArray( "virtualDocumentResolver" ), indexedField, factory );
        final int[] virtualDocumentGap = parseVirtualDocumentGap( jsapResult.getStringArray( "virtualDocumentGap" ), indexedField, factory );

        DocumentCollectionBuilder builder = null;

        run( ioFactory, jsapResult.getString( "basename" ), documentSequence, Completeness.valueOf( jsapResult.getString( "completeness" ) ), jsapResult.getBoolean( "downcase" ) ? DowncaseTermProcessor.getInstance() : ObjectParser.fromSpec( jsapResult
                .getString( "termProcessor" ), TermProcessor.class, MG4JClassParser.PACKAGE, new String[] { "getInstance" } ), builder, jsapResult
                .getInt( "bufferSize" ), batchSize, jsapResult.getInt( "maxTerms" ), indexedField, virtualDocumentResolver, virtualDocumentGap, jsapResult.getString( "renumber" ), jsapResult.getLong( "logInterval" ), jsapResult
                .getString( "tempDir" ), jsapResult.getString( "language-model" ));
    }

    public static void run( final IOFactory ioFactory, final String basename, final DocumentSequence documentSequence, final Completeness completeness, final TermProcessor termProcessor, final DocumentCollectionBuilder builder, final int bufferSize,
                            final int documentsPerBatch, final int maxTerms, final int[] indexedField, final VirtualDocumentResolver[] virtualDocumentResolver, final int[] virtualGap, final String mapFile, final long logInterval,
                            final String tempDirName, final String lmName ) throws ConfigurationException, IOException {

        final boolean building = builder != null;
        final int numberOfIndexedFields = indexedField.length;
        if ( numberOfIndexedFields == 0 ) throw new IllegalArgumentException( "You must specify at least one field" );
        final DocumentFactory factory = documentSequence.factory();
        final File tempDir = tempDirName == null ? null : new File( tempDirName );
        for ( int i = 0; i < indexedField.length; i++ )
            if ( factory.fieldType( indexedField[ i ] ) == DocumentFactory.FieldType.VIRTUAL && ( virtualDocumentResolver == null || virtualDocumentResolver[ i ] == null ) ) throw new IllegalArgumentException(
                    "No resolver was associated with virtual field " + factory.fieldName( indexedField[ i ] ) );

        if ( mapFile != null && ioFactory != IOFactory.FILESYSTEM_FACTORY ) throw new IllegalStateException( "Remapped indices currently do not support I/O factories" );
        final int[] map = mapFile != null ? BinIO.loadInts( mapFile ) : null;

        final Scan[] scan = new ExtractCorpus[1]; // To scan textual content
        // document data

        final ProgressLogger pl = new ProgressLogger( LOGGER, logInterval, TimeUnit.MILLISECONDS, "documents" );
        if ( documentSequence instanceof DocumentCollection ) pl.expectedUpdates = ( (DocumentCollection)documentSequence ).size();

        final PrintStream titleStream = new PrintStream( basename + Scan.TITLES_EXTENSION, "UTF-8" );

        final String fieldName = factory.fieldName(indexedField[0]);
        scan[0] = new ExtractCorpus(ioFactory, basename + '-' + fieldName, fieldName, completeness, termProcessor, map != null ? IndexingType.REMAPPED
             : IndexingType.STANDARD, 0, 0, bufferSize, builder, tempDir);

        pl.displayFreeMemory = true;
        pl.start( "Extracting corpus..." );

        DocumentIterator iterator = documentSequence.iterator();
        Reader reader;
        WordReader wordReader;
        List<VirtualDocumentFragment> fragments;
        Document document;

        int documentPointer = 0, documentsInBatch = 0;
        long batchStartTime = System.currentTimeMillis();
        boolean outOfMemoryError = false;
        final MutableString title = new MutableString();

        while ( ( document = iterator.nextDocument() ) != null ) {

            long overallTerms = 0;
            if ( document.title() != null ) {
                title.replace( document.title() );
                title.replace( LINE_TERMINATORS, SPACES );
                titleStream.print( title );
            }
            titleStream.println();
            reader = (Reader)document.content( indexedField[ 0] );
            wordReader = document.wordReader( indexedField[ 0 ] );
            wordReader.setReader( reader );
            if ( building ) builder.startTextField();
            scan[ 0 ].processDocument( map != null ? map[ documentPointer ] : documentPointer, wordReader );
            overallTerms += scan[ 0 ].numTerms;

            if ( scan[ 0 ] != null && scan[ 0 ].outOfMemoryError ) outOfMemoryError = true;

            documentPointer++;
            documentsInBatch++;
            document.close();
            pl.update();

            long percAvailableMemory = 100;
            boolean compacted = false;
            if ( ( documentPointer & 0xFF ) == 0 ) {
                // We try compaction if we detect less than PERC_AVAILABLE_MEMORY_CHECK memory available
                percAvailableMemory = Util.percAvailableMemory();
                if ( ! outOfMemoryError && percAvailableMemory < PERC_AVAILABLE_MEMORY_CHECK ) {
                    LOGGER.info( "Starting compaction... (" + percAvailableMemory + "% available)" );
                    compacted = true;
                    Util.compactMemory();
                    percAvailableMemory = Util.percAvailableMemory();
                    LOGGER.info( "Compaction completed (" + percAvailableMemory + "% available)" );
                }
            }

            if ( outOfMemoryError || overallTerms >= maxTerms || documentsInBatch == documentsPerBatch || ( compacted && percAvailableMemory < PERC_AVAILABLE_MEMORY_DUMP ) ) {
                if ( outOfMemoryError ) LOGGER.warn( "OutOfMemoryError during buffer reallocation: writing a batch of " + documentsInBatch + " documents" );
                else if ( overallTerms >= maxTerms ) LOGGER.warn( "Too many terms (" + overallTerms + "): writing a batch of " + documentsInBatch + " documents" );
                else if ( compacted && percAvailableMemory < PERC_AVAILABLE_MEMORY_DUMP ) LOGGER.warn( "Available memory below " + PERC_AVAILABLE_MEMORY_DUMP + "%: writing a batch of " + documentsInBatch + " documents" );

                long occurrences = 0;

                Corpus.flush();

                LOGGER.info( "Last set of corpus extracted at " + Util.format( ( 1000. * occurrences ) / ( System.currentTimeMillis() - batchStartTime ) ) + " occurrences/s" );
                batchStartTime = System.currentTimeMillis();
                documentsInBatch = 0;
                outOfMemoryError = false;
            }
        }

        iterator.close();
        titleStream.close();
        Corpus.close();
        documentSequence.close();
        pl.done();

        if ( map != null && documentPointer != map.length ) LOGGER.warn( "The document sequence contains " + documentPointer + " documents, but the map contains "
                + map.length + " integers" );
    }


}
