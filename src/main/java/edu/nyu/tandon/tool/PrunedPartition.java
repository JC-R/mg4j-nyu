package edu.nyu.tandon.tool;

/*		 
 * MG4J: Managing Gigabytes for Java (big)
 *
 * Copyright (C) 2006-2015 Sebastiano Vigna 
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
import edu.nyu.tandon.index.cluster.PostingPruningStrategy;
import it.unimi.di.big.mg4j.index.*;
import it.unimi.di.big.mg4j.index.CompressionFlags.Coding;
import it.unimi.di.big.mg4j.index.CompressionFlags.Component;
import it.unimi.di.big.mg4j.index.cluster.*;
import it.unimi.di.big.mg4j.index.payload.IntegerPayload;
import it.unimi.di.big.mg4j.index.payload.Payload;
import it.unimi.di.big.mg4j.io.IOFactory;
import it.unimi.di.big.mg4j.tool.Combine;
import it.unimi.di.big.mg4j.tool.Combine.IndexType;
import it.unimi.di.big.mg4j.tool.Merge;
import it.unimi.dsi.Util;
import it.unimi.dsi.bits.Fast;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntBigList;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import it.unimi.dsi.io.FastBufferedReader;
import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.io.OutputBitStream;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.util.*;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.ConfigurationMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.print.Doc;
import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.TimeUnit;


/**
 * Partitions an index documentally.
 * <p>
 * <p>A global index is partitioned documentally by providing a {@link DocumentalPartitioningStrategy}
 * that specifies a destination local index for each document, and a local document pointer. The global index
 * is scanned, and the postings_Global are partitioned among the local indices using the provided strategy. For instance,
 * a {@link ContiguousDocumentalStrategy} divides an index into blocks of contiguous documents.
 * <p>
 * <p>Since each local index contains a (proper) subset of the original set of documents, it contains in general a (proper)
 * subset of the terms in the global index. Thus, the local term numbers and the global term numbers will not in general coincide.
 * As a result, when a set of local indices is accessed transparently as a single index
 * using a {@link it.unimi.di.big.mg4j.index.cluster.DocumentalCluster},
 * a call to {@link it.unimi.di.big.mg4j.index.Index#documents(long)} will throw an {@link UnsupportedOperationException},
 * because there is no way to map the global term numbers to local term numbers.
 * <p>
 * <p>On the other hand, a call to {@link it.unimi.di.big.mg4j.index.Index#documents(CharSequence)} will be passed each local index to
 * build a global iterator. To speed up this phase for not-so-frequent terms, when partitioning an index you can require
 * the construction of {@linkplain BloomFilter Bloom filters} that will be used to try to avoid
 * inquiring indices that do not contain a term. The precision of the filters is settable.
 * <p>
 * <p>The property file will use a {@link it.unimi.di.big.mg4j.index.cluster.DocumentalMergedCluster} unless you provide
 * a {@link ContiguousDocumentalStrategy}, in which case a
 * {@link it.unimi.di.big.mg4j.index.cluster.DocumentalConcatenatedCluster} will be used instead. Note that there might
 * be other cases in which the latter is adapt, in which case you can edit manually the property file.
 * <p>
 * <p><strong>Important</strong>: this class just partitions the index. No auxiliary files (most notably, {@linkplain StringMap term maps}
 * or {@linkplain PrefixMap prefix maps}) will be generated. Please refer to a {@link StringMap} implementation (e.g.,
 * {@link ShiftAddXorSignedStringMap} or {@link ImmutableExternalPrefixMap}).
 * <p>
 * <p><strong>Warning</strong>: variable quanta are not supported by this class, as it is impossible to predict accurately
 * the number of bits used for positions when partitioning documentally. If you want to use variable quanta, use a
 * simple interleaved index without skips as an intermediate step, and pass it through {@link Combine}.
 * <p>
 * <h2>Sizes</h2>
 * <p>
 * <p>Partitioning the file containing document sizes is a tricky issue. For the time being this class
 * implements a very simple policy: if {@link DocumentalPartitioningStrategy#numberOfDocuments(int)} returns the number of
 * documents of the global index, the size file for a local index is generated by replacing all sizes of documents not
 * belonging to the index with a zero. Otherwise, the file is generated by appending in order the sizes of the documents
 * belonging to the index. This simple strategy works well with contiguous splitting and with splittings that do not
 * change the document numbers (e.g., the inverse operation of a {@link Merge}). However, more complex splittings might give rise
 * to inconsistent size files.
 * <p>
 * <h2>Write-once output and distributed index partitioning</h2>
 * <p>
 * Please see {@link it.unimi.di.big.mg4j.tool.PartitionLexically}&mdash;the same comments apply.
 *
 * @author Alessandro Arrabito
 * @author Sebastiano Vigna
 * @since 1.0.1
 */

public class PrunedPartition {

    /**
     * The default buffer size for all involved indices.
     */
    public final static int DEFAULT_BUFFER_SIZE = 1024 * 1024;
    private final static Logger LOGGER = LoggerFactory.getLogger(it.unimi.di.big.mg4j.tool.PartitionDocumentally.class);
    /**
     * The number of local indices.
     */
    private final int numIndices;
    /**
     * The output basenames.
     */
    private final String outputBasename;
    /**
     * The array of local output basenames.
     */
    private final String[] localBasename;
    /**
     * The input basename.
     */
    private final String inputBasename;
    /**
     * The properties of the input index.
     */
    private final Properties inputProperties;
    /**
     * The size of I/O buffers.
     */
    private final int bufferSize;
    /**
     * The filename of the strategy used to partition the index.
     */
    private final String strategyFilename;
    /**
     * The strategy used to perform the partitioning.
     */
    private final DocumentalPartitioningStrategy strategy;
    /**
     * The additional local properties of each local index.
     */
    private final Properties[] strategyProperties;
    /**
     * The logging interval.
     */
    private final long logInterval;
    /**
     * The global index to be partitioned.
     */
    private final Index globalIndex;
    /**
     * A reader on {@link #globalIndex}.
     */
    private final IndexReader indexReader;
    /**
     * A reader for the terms of the global index.
     */
    private final FastBufferedReader terms;
    /**
     * An index writer for each local index.
     */
    private final IndexWriter[] indexWriter;
    /**
     * Whether each {@link #indexWriter} has counts.
     */
    private final boolean haveCounts;
    /**
     * Whether each {@link #indexWriter} has positions.
     */
    private final boolean havePositions;
    /**
     * Whether each {@link #indexWriter} has payloads.
     */
    private final boolean havePayloads;
    /**
     * A print writer for the terms of each local index.
     */
    private final PrintWriter[] localTerms;
    /**
     * The maximum size of a document in each local index.
     */
    private final int[] maxDocSize;
    /**
     * The maximum number of positions in each local index.
     */
    private final int[] maxDocPos;
    /**
     * The number of terms in each local index.
     */
    private final long[] numTerms;
    /**
     * The number of postings_Global in each local index.
     */
    private final long[] numPostings;
    /**
     * The number of occurrences in each local index.
     */
    private final long[] numOccurrences;
    /**
     * The global count for each local index.
     */
    private final long[] occurrencies;
    /**
     * The required precision for Bloom filters (0 means no filter).
     */
    private final int bloomFilterPrecision;
    /**
     * pruned index ignores second index; track the # documents here
     */
    private final long[] numberOfDocuments;
    /**
     * A copy of {@link #indexWriter} which is non-<code>null</code> if {@link #indexWriter} is an instance of {@link QuasiSuccinctIndexWriter}[].
     */
    private QuasiSuccinctIndexWriter[] quasiSuccinctIndexWriter;

    public PrunedPartition(final String inputBasename,
                           final String outputBasename,
                           final DocumentalPartitioningStrategy strategy,
                           final String strategyFilename,
                           final int BloomFilterPrecision,
                           final int bufferSize,
                           final Map<Component, Coding> writerFlags,
                           IndexType indexType,
                           boolean skips,
                           final int quantum,
                           final int height,
                           final int skipBufferOrCacheSize,
                           final long logInterval) throws ConfigurationException, IOException, ClassNotFoundException, SecurityException, InstantiationException, IllegalAccessException, URISyntaxException, InvocationTargetException, NoSuchMethodException {

        this.inputBasename = inputBasename;
        this.outputBasename = outputBasename;
        this.strategy = strategy;
        this.strategyFilename = strategyFilename;
        this.strategyProperties = strategy.properties();
        this.bufferSize = bufferSize;
        this.logInterval = logInterval;
        this.bloomFilterPrecision = BloomFilterPrecision;

        numIndices = strategy.numberOfLocalIndices();
        if (numIndices != 2) throw new ConfigurationException("Invalid number of indeces returnd from the strategy.");

        final Coding positionCoding = writerFlags.get(Component.POSITIONS);

        inputProperties = new Properties(inputBasename + DiskBasedIndex.PROPERTIES_EXTENSION);
        globalIndex = Index.getInstance(inputBasename, false, positionCoding == Coding.GOLOMB || positionCoding == Coding.INTERPOLATIVE, false);
        indexReader = globalIndex.getReader();

        localBasename = new String[numIndices];
        for (int i = 0; i < numIndices; i++) localBasename[i] = outputBasename + "-" + i;

        localTerms = new PrintWriter[numIndices];
        maxDocSize = new int[numIndices];
        maxDocPos = new int[numIndices];
        numTerms = new long[numIndices];
        occurrencies = new long[numIndices];
        numOccurrences = new long[numIndices];
        numPostings = new long[numIndices];
        indexWriter = new IndexWriter[numIndices];
        quasiSuccinctIndexWriter = new QuasiSuccinctIndexWriter[numIndices];

        this.numberOfDocuments = new long[2];
        numberOfDocuments[0] = strategy.numberOfDocuments(0);
        numberOfDocuments[1] = globalIndex.numberOfDocuments - numberOfDocuments[0];

        if ((havePayloads = writerFlags.containsKey(Component.PAYLOADS)) && !globalIndex.hasPayloads)
            throw new IllegalArgumentException("You requested payloads, but the global index does not contain them.");
        if ((haveCounts = writerFlags.containsKey(Component.COUNTS)) && !globalIndex.hasCounts)
            throw new IllegalArgumentException("You requested counts, but the global index does not contain them.");
        if (!globalIndex.hasPositions && writerFlags.containsKey(Component.POSITIONS))
            writerFlags.remove(Component.POSITIONS);
        if ((havePositions = writerFlags.containsKey(Component.POSITIONS)) && !globalIndex.hasPositions)
            throw new IllegalArgumentException("You requested positions, but the global index does not contain them.");
        if (indexType == IndexType.HIGH_PERFORMANCE && !havePositions)
            throw new IllegalArgumentException("You cannot disable positions for high-performance indices.");
        if (indexType != IndexType.INTERLEAVED && havePayloads)
            throw new IllegalArgumentException("Payloads are available in interleaved indices only.");
        skips |= indexType == IndexType.HIGH_PERFORMANCE;

        if (skips && (quantum <= 0 || height < 0))
            throw new IllegalArgumentException("You must specify a positive quantum and a nonnegative height (variable quanta are not available when partitioning documentally).");

        // we only produce 1 index
            switch (indexType) {
                case INTERLEAVED:
                    if (!skips)
                        indexWriter[0] = new BitStreamIndexWriter(IOFactory.FILESYSTEM_FACTORY, localBasename[0], numberOfDocuments[0], true, writerFlags);
                    else
                        indexWriter[0] = new SkipBitStreamIndexWriter(IOFactory.FILESYSTEM_FACTORY, localBasename[0], numberOfDocuments[0], true, skipBufferOrCacheSize, writerFlags, quantum, height);
                    break;
                case HIGH_PERFORMANCE:
                    indexWriter[0] = new BitStreamHPIndexWriter(localBasename[0], numberOfDocuments[0], true, skipBufferOrCacheSize, writerFlags, quantum, height);
                    break;
                case QUASI_SUCCINCT:
                    quasiSuccinctIndexWriter[0] = (QuasiSuccinctIndexWriter) (indexWriter[0] = new QuasiSuccinctIndexWriter(IOFactory.FILESYSTEM_FACTORY, localBasename[0], numberOfDocuments[0], Fast.mostSignificantBit(quantum < 0 ? QuasiSuccinctIndex.DEFAULT_QUANTUM : quantum), skipBufferOrCacheSize, writerFlags, ByteOrder.nativeOrder()));
            }
        localTerms[0] = new PrintWriter(new BufferedWriter(new OutputStreamWriter(new FileOutputStream(localBasename[0] + DiskBasedIndex.TERMS_EXTENSION), "UTF-8")));

        terms = new FastBufferedReader(new InputStreamReader(new FileInputStream(inputBasename + DiskBasedIndex.TERMS_EXTENSION), "UTF-8"));

    }

    public static void main(final String arg[]) throws Exception {

        SimpleJSAP jsap = new SimpleJSAP(it.unimi.di.big.mg4j.tool.PartitionDocumentally.class.getName(), "Partitions an index documentally.",
                new Parameter[]{
                        new FlaggedOption("bufferSize", JSAP.INTSIZE_PARSER, Util.formatBinarySize(DEFAULT_BUFFER_SIZE), JSAP.NOT_REQUIRED, 'b', "buffer-size", "The size of an I/O buffer."),
                        new FlaggedOption("logInterval", JSAP.LONG_PARSER, Long.toString(ProgressLogger.DEFAULT_LOG_INTERVAL), JSAP.NOT_REQUIRED, 'l', "log-interval", "The minimum time interval between activity logs in milliseconds."),
                        new FlaggedOption("strategy", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 's', "strategy", "A serialised documental partitioning strategy."),
                        new FlaggedOption("uniformStrategy", JSAP.INTEGER_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'u', "uniform", "Requires a uniform partitioning in the given number of parts."),
                        new FlaggedOption("bloom", JSAP.INTEGER_PARSER, "0", JSAP.NOT_REQUIRED, 'B', "bloom", "Generates Bloom filters with given precision."),
                        new FlaggedOption("comp", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'c', "comp", "A compression flag for the index (may be specified several times).").setAllowMultipleDeclarations(true),
                        new Switch("noSkips", JSAP.NO_SHORTFLAG, "no-skips", "Disables skips."),
                        new Switch("interleaved", JSAP.NO_SHORTFLAG, "interleaved", "Forces an interleaved index."),
                        new Switch("highPerformance", 'h', "high-performance", "Forces a high-performance index."),
                        new FlaggedOption("cacheSize", JSAP.INTSIZE_PARSER, Util.formatBinarySize(QuasiSuccinctIndexWriter.DEFAULT_CACHE_SIZE), JSAP.NOT_REQUIRED, JSAP.NO_SHORTFLAG, "cache-size", "The size of the bit cache used while creating a quasi-succinct index."),
                        new FlaggedOption("quantum", JSAP.INTSIZE_PARSER, "32", JSAP.NOT_REQUIRED, 'Q', "quantum", "The skip quantum."),
                        new FlaggedOption("stopwords", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'S', "stopword", "The stop words."),
                        new FlaggedOption("height", JSAP.INTSIZE_PARSER, Integer.toString(BitStreamIndex.DEFAULT_HEIGHT), JSAP.NOT_REQUIRED, 'H', "height", "The skip height."),
                        new FlaggedOption("skipBufferSize", JSAP.INTSIZE_PARSER, Util.formatBinarySize(SkipBitStreamIndexWriter.DEFAULT_TEMP_BUFFER_SIZE), JSAP.NOT_REQUIRED, JSAP.NO_SHORTFLAG, "skip-buffer-size", "The size of the internal temporary buffer used while creating an index with skips."),
                        new UnflaggedOption("inputBasename", JSAP.STRING_PARSER, JSAP.REQUIRED, "The basename of the global index."),
                        new FlaggedOption("outputBasename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'o', "The basename of the local indices.")
                });

        JSAPResult jsapResult = jsap.parse(arg);
        if (jsap.messagePrinted()) return;
        String inputBasename = jsapResult.getString("inputBasename");
        String outputBasename = jsapResult.getString("outputBasename");
        String strategyFilename = jsapResult.getString("strategy");
        DocumentalPartitioningStrategy strategy = null;


        if (jsapResult.userSpecified("uniformStrategy")) {
            strategy = DocumentalStrategies.uniform(jsapResult.getInt("uniformStrategy"), Index.getInstance(inputBasename).numberOfDocuments);
            BinIO.storeObject(strategy, strategyFilename = outputBasename + IndexCluster.STRATEGY_DEFAULT_EXTENSION);
        } else if (strategyFilename != null)
            strategy = (DocumentalPartitioningStrategy) BinIO.loadObject(strategyFilename);
        else throw new IllegalArgumentException("You must specify a partitioning strategy");

        final boolean skips = !jsapResult.getBoolean("noSkips");
        final boolean interleaved = jsapResult.getBoolean("interleaved");
        final boolean highPerformance = jsapResult.getBoolean("highPerformance");
        if (!skips && !interleaved)
            throw new IllegalArgumentException("You can disable skips only for interleaved indices");
        if (interleaved && highPerformance)
            throw new IllegalArgumentException("You must specify either --interleaved or --high-performance.");
        if (!skips && (jsapResult.userSpecified("quantum") || jsapResult.userSpecified("height")))
            throw new IllegalArgumentException("You specified quantum or height, but you also disabled skips.");

        final IndexType indexType = interleaved ? IndexType.INTERLEAVED : highPerformance ? IndexType.HIGH_PERFORMANCE : IndexType.QUASI_SUCCINCT;

        final Map<Component, Coding> compressionFlags = indexType == IndexType.QUASI_SUCCINCT ?
                CompressionFlags.valueOf(jsapResult.getStringArray("comp"), CompressionFlags.DEFAULT_QUASI_SUCCINCT_INDEX) :
                CompressionFlags.valueOf(jsapResult.getStringArray("comp"), CompressionFlags.DEFAULT_STANDARD_INDEX);

        new PrunedPartition(inputBasename,
                outputBasename,
                strategy,
                strategyFilename,
                jsapResult.getInt("bloom"),
                jsapResult.getInt("bufferSize"),
                compressionFlags,
                indexType,
                skips,
                jsapResult.getInt("quantum"),
                jsapResult.getInt("height"),
                indexType == IndexType.QUASI_SUCCINCT ? jsapResult.getInt("cacheSize") : jsapResult.getInt("skipBufferSize"),
                jsapResult.getLong("logInterval")).run();
    }

    private void partitionSizes() throws IOException {

        final File sizesFile = new File(inputBasename + DiskBasedIndex.SIZES_EXTENSION);

        if (sizesFile.exists()) {

            LOGGER.info("Partitioning sizes...");
            final InputBitStream sizes = new InputBitStream(sizesFile);
            final OutputBitStream localSizes = new OutputBitStream(localBasename[0] + DiskBasedIndex.SIZES_EXTENSION);

            // WARN: can only handle 2.5billion documents; should be OK
            int[] localDocSize = new int[(int) strategy.numberOfDocuments(0)];

            // ALERT: for the time being, we decide whether to "fill the gaps" in sizes using as sole indicator the equality between global and local number of documents.
            int size, localIndex, localID;
            int currdoc = 0;

            if (globalIndex.numberOfDocuments == strategy.numberOfDocuments(0)) {
                for (int i = 0; i < globalIndex.numberOfDocuments; i++) {
                    localIndex = strategy.localIndex(i);
                    size = sizes.readGamma();
                    localDocSize[localID = (int) strategy.localPointer(i)] = (localIndex == 0) ? size : 0;
                    if (maxDocSize[localIndex] < size) maxDocSize[localIndex] = size;
                }
            } else {
                for (int i = 0; i < globalIndex.numberOfDocuments; i++) {
                    localIndex = strategy.localIndex(i);
                    size = sizes.readGamma();
                    if (localIndex == 0) localDocSize[localID = (int) strategy.localPointer(i)] = size;
                    if (maxDocSize[localIndex] < size) maxDocSize[localIndex] = size;
                }
            }
            // write documents in local numbering
            for (int i = 0; i < strategy.numberOfDocuments(0); i++) {
                localSizes.writeGamma(localDocSize[i]);
            }
            sizes.close();
            localSizes.close();
        }
    }

    public void run() throws Exception {

        final ProgressLogger pl = new ProgressLogger(LOGGER, logInterval, TimeUnit.MILLISECONDS);
        final IntBigList sizeList = globalIndex.sizes;

        partitionSizes();

        final Long2LongOpenHashMap documents = new Long2LongOpenHashMap();

        long localFrequency = 0;
        long sumMaxPos = 0;

        InputBitStream direct;
        InputBitStream indirect;
        @SuppressWarnings("unchecked")
        BloomFilter<Void> bloomFilter;

        final File tempFile;
        final CachingOutputBitStream temp;

        final File orderFile;
        final CachingOutputBitStream order;

        long lID;

        IndexIterator indexIterator;

        bloomFilter = (bloomFilterPrecision != 0) ?
                BloomFilter.create(globalIndex.numberOfTerms, bloomFilterPrecision) : null;

        MutableString currentTerm = new MutableString();
        Payload payload = null;
        long frequency, globalPointer, localPointer, termID;
        int localIndex, count = -1;

        pl.expectedUpdates = globalIndex.numberOfPostings;
        pl.itemsName = "postings";
        pl.logInterval = logInterval;
        pl.start("Partitioning index...");

        final OutputBitStream globalFrequencies = new OutputBitStream(localBasename[0] + ".globaltermfreq");

        // for now, we rebuild the list in memory : TODO: fix so any size list is possible
        class DocEntry {
            long docID;
            Payload payload;
            int count;
            int[] pos;
        }
        Long2ObjectOpenHashMap<DocEntry> list = new Long2ObjectOpenHashMap<DocEntry>();
        list.clear();

        for (long t = 0; t < globalIndex.numberOfTerms; t++) {

            terms.readLine(currentTerm);

            indexIterator = indexReader.nextIterator();
            frequency = indexIterator.frequency();
            termID = indexIterator.termNumber();
            assert termID == t;

            localFrequency = 0;

            IntegerPayload payload1;

            // if the term never made it to the pruned index; skip it
            if ((lID = ((PostingPruningStrategy) strategy).localTermId(termID)) == -1) continue;

            for (long j = 0; j < frequency; j++) {

                globalPointer = indexIterator.nextDocument();

                // (term,doc) in the pruned index?
                if ((localIndex = ((PostingPruningStrategy) strategy).localIndex(termID, globalPointer)) == 0) {

                    // First time this term is seen
                    if (localFrequency == 0) {
//                        assert numTerms[0] == ((PostingPruningStrategy) strategy).localTermId(termID);
                        numTerms[0]++;
                        currentTerm.println(localTerms[localIndex]);        // save term
                        globalFrequencies.writeLongGamma(frequency);        // save original term size
                        if (bloomFilterPrecision != 0) bloomFilter.add(currentTerm);
                    }

                    /* Store temporarily posting data; note that we save the global pointer as we
                     * will have to access the size list. */
                    // local docID is written in later...
                    //
                    if (globalIndex.hasPayloads) payload = indexIterator.payload();

                    DocEntry d = new DocEntry();
                    d.docID = globalPointer;
                    if (globalIndex.hasPayloads) payload = indexIterator.payload();
                    d.payload = (havePayloads) ? payload : null;
                    count = (haveCounts) ? indexIterator.count() : 0;
                    d.count = count;

                    numPostings[0]++;

                    if (haveCounts) {
                        occurrencies[localIndex] += count;
                        if (maxDocPos[localIndex] < count) maxDocPos[localIndex] = count;
                        if (havePositions) {
                            d.pos = new int[count];
                            for (int p = 0; p < count; p++) {
                                int pos = indexIterator.nextPosition();
                                d.pos[p] = pos;
                                sumMaxPos += pos;
                            }
                        }
                    }

                    localFrequency++;
                    list.put(strategy.localPointer(globalPointer), d);
                } else {
                    // synchronize aux files
                    if (globalIndex.hasPayloads) payload = indexIterator.payload();
                    if (haveCounts) {
                        count = indexIterator.count();
                        if (havePositions) {
                            for (int p = 0; p < count; p++) {
                                int pos = indexIterator.nextPosition();
                            }
                        }
                    }
                }
            }

            // We now run through the pruned index and copy from the temporary buffer.
            OutputBitStream obs;

            // list will not be ordered anymore, since we will remap to local docIDs.
            // and the local docIDs were assigned by the strategy based on the strategy order (hits, etc)

            if (localFrequency>0) {

                if (haveCounts) numOccurrences[0] += occurrencies[0];

                // create a post list
                if (quasiSuccinctIndexWriter[0] != null)
                    quasiSuccinctIndexWriter[0].newInvertedList(localFrequency, occurrencies[0], sumMaxPos);
                else indexWriter[0].newInvertedList();

                occurrencies[0] = 0;

                indexWriter[0].writeFrequency(localFrequency);

                // we want the index list in local docID order
                long[] docs = list.keySet().toLongArray();
                Arrays.sort(docs);
                for (long localID : docs) {

                    DocEntry d = list.get(localID);
                    globalPointer = d.docID;
                    if (havePayloads) payload = d.payload;
                    if (haveCounts) count = d.count;

                    // TODO: support positions

                    // at the position we need
                    obs = indexWriter[0].newDocumentRecord();

                    // map from global docID to local docID
//                    localPointer = strategy.localPointer(globalPointer);
//                    assert localID == localPointer;
                    indexWriter[0].writeDocumentPointer(obs, localID);

                    if (havePayloads) {
                        indexWriter[0].writePayload(obs, payload);
                    }

                    if (haveCounts) indexWriter[0].writePositionCount(obs, count);
                    if (havePositions) {
                        indexWriter[0].writeDocumentPositions(obs, d.pos, 0, count, sizeList != null ? sizeList.getInt(globalPointer) : -1);
                    }
                }

                sumMaxPos = 0;
            } else {
                sumMaxPos = 0;
            }
            localFrequency = 0;
            pl.count += frequency - 1;
            pl.update();
            list.clear();

        }
        globalFrequencies.close();

        pl.done();

        Properties globalProperties = new Properties();
        globalProperties.setProperty(Index.PropertyKeys.FIELD, inputProperties.getProperty(Index.PropertyKeys.FIELD));
        globalProperties.setProperty(Index.PropertyKeys.TERMPROCESSOR, inputProperties.getProperty(Index.PropertyKeys.TERMPROCESSOR));

        localTerms[0].close();
        indexWriter[0].close();
        if (bloomFilterPrecision != 0)
            BinIO.storeObject(bloomFilter, localBasename[0] + DocumentalCluster.BLOOM_EXTENSION);

        Properties localProperties = indexWriter[0].properties();
        localProperties.addAll(globalProperties);
        localProperties.setProperty(Index.PropertyKeys.MAXCOUNT, String.valueOf(maxDocPos[0]));
        localProperties.setProperty(Index.PropertyKeys.MAXDOCSIZE, maxDocSize[0]);
        localProperties.setProperty(Index.PropertyKeys.FIELD, globalProperties.getProperty(Index.PropertyKeys.FIELD));
        localProperties.setProperty(Index.PropertyKeys.OCCURRENCES, haveCounts ? numOccurrences[0] : -1);
        localProperties.setProperty(Index.PropertyKeys.POSTINGS, numPostings[0]);
        localProperties.setProperty(Index.PropertyKeys.TERMS, numTerms[0]);
        if (havePayloads)
            localProperties.setProperty(Index.PropertyKeys.PAYLOADCLASS, payload.getClass().getName());
        if (strategyProperties != null && strategyProperties[0] != null)
            localProperties.addAll(strategyProperties[0]);
        // add global properties
        localProperties.addProperty(globalPropertyKeys.G_MAXCOUNT, inputProperties.getProperty(Index.PropertyKeys.MAXCOUNT));
        localProperties.addProperty(globalPropertyKeys.G_MAXDOCSIZE, inputProperties.getProperty(Index.PropertyKeys.MAXDOCSIZE));
        localProperties.addProperty(globalPropertyKeys.G_POSTINGS, inputProperties.getProperty(Index.PropertyKeys.POSTINGS));
        localProperties.addProperty(globalPropertyKeys.G_OCCURRENCES, inputProperties.getProperty(Index.PropertyKeys.OCCURRENCES));
        localProperties.addProperty(globalPropertyKeys.G_DOCUMENTS, inputProperties.getProperty(Index.PropertyKeys.DOCUMENTS));
        localProperties.addProperty(globalPropertyKeys.G_TERMS, inputProperties.getProperty(Index.PropertyKeys.TERMS));

        localProperties.save(localBasename[0] + DiskBasedIndex.PROPERTIES_EXTENSION);

        if (strategyFilename != null)
            globalProperties.setProperty(IndexCluster.PropertyKeys.STRATEGY, strategyFilename);
        globalProperties.addProperty(IndexCluster.PropertyKeys.LOCALINDEX, localBasename[0]);
        globalProperties.setProperty(DocumentalCluster.PropertyKeys.BLOOM, bloomFilterPrecision != 0);
        // If we partition an index with a single term, by definition we have a flat cluster
        globalProperties.setProperty(DocumentalCluster.PropertyKeys.FLAT, inputProperties.getLong(Index.PropertyKeys.TERMS) <= 1);
        globalProperties.setProperty(Index.PropertyKeys.MAXCOUNT, inputProperties.getProperty(Index.PropertyKeys.MAXCOUNT));
        globalProperties.setProperty(Index.PropertyKeys.MAXDOCSIZE, inputProperties.getProperty(Index.PropertyKeys.MAXDOCSIZE));
        globalProperties.setProperty(Index.PropertyKeys.POSTINGS, inputProperties.getProperty(Index.PropertyKeys.POSTINGS));
        globalProperties.setProperty(Index.PropertyKeys.OCCURRENCES, inputProperties.getProperty(Index.PropertyKeys.OCCURRENCES));
        globalProperties.setProperty(Index.PropertyKeys.DOCUMENTS, inputProperties.getProperty(Index.PropertyKeys.DOCUMENTS));
        globalProperties.setProperty(Index.PropertyKeys.TERMS, inputProperties.getProperty(Index.PropertyKeys.TERMS));
        if (havePayloads) globalProperties.setProperty(Index.PropertyKeys.PAYLOADCLASS, payload.getClass().getName());

		/* For the general case, we must rely on a merged cluster. However, if we detect a contiguous
         * strategy we can optimise a bit. */

        globalProperties.setProperty(Index.PropertyKeys.INDEXCLASS,
                strategy instanceof ContiguousDocumentalStrategy ?
                        DocumentalConcatenatedCluster.class.getName() :
                        DocumentalMergedCluster.class.getName());

        globalProperties.save(outputBasename + DiskBasedIndex.PROPERTIES_EXTENSION);
        LOGGER.debug("Properties for clustered index " + outputBasename + ": " + new ConfigurationMap(globalProperties));

    }

    /**
     * Symbolic names for global metrics
     */
    public static enum globalPropertyKeys {
        /** The number of documents in the collection. */
        G_DOCUMENTS,
        /** The number of terms in the collection. */
        G_TERMS,
        /** The number of occurrences in the collection, or -1 if the number of occurrences is not known. */
        G_OCCURRENCES,
        /** The number of postings (pairs term/document) in the collection. */
        G_POSTINGS,
        /** The number of batches this index was (or should be) built from. */
        G_MAXCOUNT,
        /** The maximum size (in words) of a document, or -1 if the maximum document size is not known. */
        G_MAXDOCSIZE
    }
}
