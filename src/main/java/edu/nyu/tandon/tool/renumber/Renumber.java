package edu.nyu.tandon.tool.renumber;

import com.google.common.math.DoubleMath;
import com.martiansoftware.jsap.*;
import edu.nyu.tandon.utils.Utils;
import it.unimi.di.big.mg4j.index.*;
import it.unimi.di.big.mg4j.index.payload.Payload;
import it.unimi.di.big.mg4j.io.IOFactory;
import it.unimi.di.big.mg4j.tool.Scan;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.longs.LongBigArrayBigList;
import it.unimi.dsi.fastutil.longs.LongBigArrays;
import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.io.OutputBitStream;
import it.unimi.dsi.util.Properties;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.lang.time.DurationFormatUtils;
import org.codehaus.plexus.util.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static it.unimi.di.big.mg4j.index.DiskBasedIndex.*;
import static it.unimi.di.big.mg4j.index.IndexIterator.END_OF_LIST;
import static it.unimi.dsi.fastutil.io.BinIO.loadLongsBig;
import static java.lang.Integer.valueOf;

/**
 * @author michal.siedlaczek@nyu.edu
 *         <p>
 *         This class copies an index while renumbering the document IDs according to a map file.
 *         NOTE: So far, this works only for QuasiSuccinctIndex without positions.
 */
public class Renumber {

    public static final Logger LOGGER = LoggerFactory.getLogger(Renumber.class);

    public static final String MWHC_EXTENSION = ".mwhc";

    protected long writtenCounts = 0;
    protected String inputBasename;
    protected String outputBasename;
    protected Index index;
    protected IndexReader indexReader;
    protected IndexWriter indexWriter;
    protected QuasiSuccinctIndexWriter qsIndexWriter;
    protected IOFactory ioFactory = IOFactory.FILESYSTEM_FACTORY;
    /**
     * Mapping of document IDs: i-th document will be remapped to mapping[i].
     */
    protected LongBigArrayBigList mapping;

    public Renumber(String inputBasename, String outputBasename) throws IOException, IllegalAccessException, InvocationTargetException, InstantiationException, NoSuchMethodException, URISyntaxException, ConfigurationException, ClassNotFoundException {
        index = Index.getInstance(inputBasename);
        indexReader = index.getReader();
        this.inputBasename = inputBasename;
        this.outputBasename = outputBasename;
        instantiateWriter(outputBasename);
    }

    public static void main(String[] args) throws IllegalAccessException, URISyntaxException, IOException, InstantiationException, NoSuchMethodException, ConfigurationException, InvocationTargetException, ClassNotFoundException, JSAPException {

        SimpleJSAP jsap = new SimpleJSAP(Renumber.class.getName(), "",
                new Parameter[]{
                        new FlaggedOption("inputBasename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'i', "input-basename", "The basename of the index to be renumbered."),
                        new FlaggedOption("outputBasename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'o', "output-basename", "The basename of the renumbered index."),
                        new FlaggedOption("mapFile", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'm', "map-file", "The file containing mapping between document IDs."),
                        new Switch("binaryMapping", 'b', "binary-mapping", "When present, provided mapping is a binary list of integers instead of (by default) a text file containing numbers in consecutive lines.")
                });

        JSAPResult jsapResult = jsap.parse(args);
        if (jsap.messagePrinted()) return;

        LOGGER.info(String.format("Renumbering %s to %s", jsapResult.getString("inputBasename"), jsapResult.getString("outputBasename")));

        Renumber renumber = new Renumber(jsapResult.getString("inputBasename"), jsapResult.getString("outputBasename"));
        renumber.readMapping(jsapResult.getString("mapFile"), jsapResult.userSpecified("binaryMapping"));
        renumber.run();
    }

    public void instantiateWriter(String basename) throws IOException, ClassNotFoundException {

        Class<?> indexClass = Class.forName(index.properties.getString(Index.PropertyKeys.INDEXCLASS));

        if (indexClass == QuasiSuccinctIndex.class) {
            qsIndexWriter = new QuasiSuccinctIndexWriter(ioFactory,
                    basename,
                    index.numberOfDocuments,
                    (int) Math.round(DoubleMath.log2(valueOf((String) index.properties.getProperty(BitStreamIndex.PropertyKeys.SKIPQUANTUM)))),
                    QuasiSuccinctIndexWriter.DEFAULT_CACHE_SIZE,
                    CompressionFlags.valueOf(new String[]{"POSITIONS:NONE"}, CompressionFlags.DEFAULT_STANDARD_INDEX),
                    ByteOrder.nativeOrder());
            indexWriter = qsIndexWriter;
        } else {
            // TODO: Other types of indices
            throw new IllegalArgumentException(String.format("Index of type %s is not supported.", indexClass.getName()));
        }
    }

    public void run() throws IOException, ConfigurationException {

        if (mapping == null) throw new IllegalStateException("Mapping file is undefined.");

        LOGGER.info(String.format("Copying inverted lists"));

        long start = System.currentTimeMillis();
        IndexIterator indexIterator = indexReader.nextIterator();
        while (indexIterator != null) {
            int i = 0;
            while (indexIterator != null && i++ < 1000) {
                writeReorderedList(indexIterator);
                indexIterator = indexReader.nextIterator();
            }
            long elapsed = System.currentTimeMillis() - start;
            long left = (index.numberOfTerms - i) / i * elapsed;
            LOGGER.debug(String.format("Copied %d terms. Elapsed time: %s. Estimated time left: %s.", i,
                    DurationFormatUtils.formatDurationHMS(elapsed),
                    DurationFormatUtils.formatDurationHMS(left)));
        }

        LOGGER.info(String.format("Copying sizes"));
        writeSizes();
        LOGGER.info(String.format("Copying properties"));
        writeProperties();
        LOGGER.info(String.format("Copying terms"));
        copyTerms();
        indexWriter.close();

    }

    public void copyTerms() throws IOException {
        FileUtils.copyFile(new File(inputBasename + TERMS_EXTENSION), new File(outputBasename + TERMS_EXTENSION));
        FileUtils.copyFile(new File(inputBasename + TERMMAP_EXTENSION), new File(outputBasename + TERMMAP_EXTENSION));
        FileUtils.copyFile(new File(inputBasename + MWHC_EXTENSION), new File(outputBasename + MWHC_EXTENSION));
    }

    public void writeSizes() throws IOException {
        try (
                InputBitStream in = new InputBitStream(ioFactory.getInputStream(inputBasename + SIZES_EXTENSION), false);
                OutputBitStream out = new OutputBitStream(ioFactory.getOutputStream(outputBasename + SIZES_EXTENSION), false)
        ) {

            LongBigArrayBigList sizes = new LongBigArrayBigList(index.numberOfDocuments);
            sizes.size(index.numberOfDocuments);
            int i;
            for (i = 0; i < index.numberOfDocuments; i++) {
                sizes.set(mapping.getLong(i), in.readGamma());
            }
            for (i = 0; i < index.numberOfDocuments; i++) {
                out.writeLongGamma(sizes.getLong(i));
            }
        }
    }

    private void writeProperties() throws IOException, ConfigurationException {
        Properties properties = indexWriter.properties();
        properties.setProperty(Index.PropertyKeys.TERMPROCESSOR, index.termProcessor.getClass().getName());
        properties.setProperty(Index.PropertyKeys.BATCHES, index.properties.getProperty(Index.PropertyKeys.BATCHES));
        properties.setProperty(Index.PropertyKeys.FIELD, index.properties.getProperty(Index.PropertyKeys.FIELD));
        properties.setProperty(Index.PropertyKeys.SIZE, indexWriter.writtenBits());
        properties.setProperty(Index.PropertyKeys.MAXCOUNT, index.properties.getProperty(Index.PropertyKeys.MAXCOUNT));
        properties.setProperty(Index.PropertyKeys.MAXDOCSIZE, index.properties.getProperty(Index.PropertyKeys.MAXDOCSIZE));
        properties.setProperty(Index.PropertyKeys.OCCURRENCES, index.properties.getProperty(Index.PropertyKeys.OCCURRENCES));
        properties.setProperty(BitStreamIndex.PropertyKeys.SKIPQUANTUM, index.properties.getProperty(BitStreamIndex.PropertyKeys.SKIPQUANTUM));
        Scan.saveProperties(ioFactory,
                properties,
                outputBasename + PROPERTIES_EXTENSION);
    }

    private void writeReorderedList(IndexIterator indexIterator) throws IOException {

        InvertedList invertedList = readList(indexIterator);
        startInvertedList(invertedList);
        writeRecords(invertedList);

    }

    public InvertedList readList(IndexIterator indexIterator) throws IOException {

        long frequency = indexIterator.frequency();

        LongBigArrayBigList documents = new LongBigArrayBigList(frequency);
        documents.size(frequency);
        Map<Long, Posting> postings = new HashMap<>();
        long sumMaxPos = 0, occurrency = 0;

        long doc;
        int i = 0;
        while ((doc = indexIterator.nextDocument()) != END_OF_LIST) {
            long mappedId = mapping.getLong(doc);
            documents.set(i++, mappedId);

            Posting posting = new Posting();
            if (index.hasPayloads) posting.payload = indexIterator.payload();
            if (index.hasCounts) {
                posting.positionCount = indexIterator.count();
                occurrency += posting.positionCount;
            }
            if (index.hasPositions) {
                posting.positions = IndexIterators.positionArray(indexIterator);
                sumMaxPos += posting.positions[posting.positions.length - 1];
                // Sizes are copied independently.
                posting.documentSize = -1;
            }

            postings.put(mappedId, posting);
        }
        LongBigArrays.quickSort(documents.elements());

        return new InvertedList(documents, postings, frequency, occurrency, sumMaxPos);
    }

    public void startInvertedList(InvertedList invertedList) throws IOException {
        if (qsIndexWriter != null) {
            qsIndexWriter.newInvertedList(invertedList.frequency,
                    index.hasCounts ? invertedList.occurrency : -1,
                    1);
            writtenCounts++;
        } else {
            indexWriter.newInvertedList();
        }
        indexWriter.writeFrequency(invertedList.frequency);
    }

    public void writeRecords(InvertedList invertedList) throws IOException {
        for (long document : invertedList.documents) {
            Posting p = invertedList.postings.get(document);
            OutputBitStream out = indexWriter.newDocumentRecord();
            indexWriter.writeDocumentPointer(out, document);
            if (index.hasPayloads) indexWriter.writePayload(out, p.payload);
            if (index.hasCounts) indexWriter.writePositionCount(out, p.positionCount);
            if (index.hasPositions) {
                indexWriter.writeDocumentPositions(out, p.positions, 0, p.positionCount, p.documentSize);
            }
        }
    }

    public void readMapping(String mapFile, boolean binary) throws IOException {
        try {
            if (binary) {
                mapping = new LongBigArrayBigList(loadLongsBig(mapFile));
            } else {
                mapping = Utils.readMapping(mapFile, index.numberOfDocuments);
            }
        } catch (IOException e) {
            throw new RuntimeException("Error while reading mapping", e);
        }
    }

    private static class Posting {

        public Payload payload;
        public int positionCount;
        public int[] positions;
        public int documentSize;

    }

    private static class InvertedList {

        public long frequency;
        public long occurrency;
        public long sumMaxPos;
        public LongBigArrayBigList documents;
        public Map<Long, Posting> postings;

        public InvertedList(LongBigArrayBigList documents, Map<Long, Posting> postings, long frequency, long occurrency, long sumMaxPos) {
            this.documents = documents;
            this.postings = postings;
            this.frequency = frequency;
            this.occurrency = occurrency;
            this.sumMaxPos = sumMaxPos;
        }

    }

}
