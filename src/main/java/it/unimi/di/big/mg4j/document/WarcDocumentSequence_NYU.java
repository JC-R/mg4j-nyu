package it.unimi.di.big.mg4j.document;

/*		 
 * MG4J: Managing Gigabytes for Java (big)
 * 
 * Copyright (C) 2015 Sebastiano Vigna 
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
import it.unimi.di.law.bubing.parser.HTMLParser;
import it.unimi.di.law.warc.io.UncompressedWarcReader_NYU;
import it.unimi.di.law.warc.io.WarcReader;
import it.unimi.di.law.warc.records.HttpResponseWarcRecord;
import it.unimi.di.law.warc.records.WarcHeader.Name;
import it.unimi.di.law.warc.records.WarcRecord;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.io.FastBufferedInputStream;
import it.unimi.dsi.fastutil.io.InspectableFileCachedInputStream;
import it.unimi.dsi.fastutil.objects.Reference2ObjectMap;
import it.unimi.dsi.fastutil.objects.Reference2ObjectOpenHashMap;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.HashSet;
import java.util.Set;
import java.util.zip.GZIPInputStream;

/**
 * A {@linkplain DocumentSequence document sequence} over a set of
 * (possibly compressed) Warc files.
 * <p>
 * <p>The metadata provided by the sequence include the {@linkplain PropertyBasedDocumentFactory.MetadataKeys#ENCODING encoding},
 * the {@linkplain PropertyBasedDocumentFactory.MetadataKeys#URI URI},
 * the {@linkplain PropertyBasedDocumentFactory.MetadataKeys#MIMETYPE MIME type}.
 * <p>
 * <p>If a Warc header with name &ldquo;WARC-TREC-ID&rdquo; is present, it will be used as
 * {@linkplain PropertyBasedDocumentFactory.MetadataKeys#TITLE TITLE}.
 * <p>
 * <p>This class will also fetch and use the {@linkplain Name#BUBING_GUESSED_CHARSET BUbiNG guessed charset}, if present.
 * <p>
 * <p>As a commodity, this class provides a main method for the creation of a serialized version
 * of the document sequence.
 */

public class WarcDocumentSequence_NYU extends AbstractDocumentSequence implements Serializable {

    /**
     * Default buffer size, set up after some edu.nyu.tandon.experiments.
     */
    public static final String DEFAULT_BUFFER_SIZE = "64Ki";
    private static final long serialVersionUID = 0L;
    private static final Logger LOGGER = LoggerFactory.getLogger(WarcDocumentSequence_NYU.class);
    /**
     * The user specified factory.
     */
    protected final DocumentFactory factory;
    /**
     * The buffer size used for reads.
     */
    protected final int bufferSize;
    /**
     * Whether the Warcfile are gzipped.
     */
    protected final boolean useGzip;
    /**
     * The list of WARC files
     */
    protected final String[] warcFile;
    protected Set<String> spamDocuments;

    protected WarcDocumentSequence_NYU(final WarcDocumentSequence_NYU prototype) {
        this.factory = prototype.factory;
        this.warcFile = prototype.warcFile;
        this.useGzip = prototype.useGzip;
        this.bufferSize = prototype.bufferSize;
        this.spamDocuments = prototype.spamDocuments;
    }

    public WarcDocumentSequence_NYU(final String[] warcFile, final DocumentFactory factory, final boolean useGzip, final int bufferSize, final String spamFile) {
        this.warcFile = warcFile;
        this.useGzip = useGzip;
        this.bufferSize = bufferSize;
        this.factory = factory;
        if (spamFile != null) {
            try (BufferedReader lines = new BufferedReader(new FileReader(spamFile))) {
                this.spamDocuments = new HashSet<>();
                String doc;
                while ((doc = lines.readLine()) != null) {
                    this.spamDocuments.add(doc);
                }
            } catch (IOException e) {
                this.spamDocuments = null;
            }
        }
    }

    public static void main(String[] args) throws Exception {

        SimpleJSAP jsap = new SimpleJSAP(WarcDocumentSequence_NYU.class.getName(), "Saves a serialised Warc document sequence based on a set of file names.", new Parameter[]{
                new FlaggedOption("factory", JSAP.CLASS_PARSER, IdentityDocumentFactory.class.getName(), JSAP.NOT_REQUIRED, 'f', "factory", "A document factory with a standard constructor."),
                new FlaggedOption("property", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'p', "property", "A 'key=value' specification, or the name of a property file").setAllowMultipleDeclarations(true),
                new Switch("gzip", 'z', "gzip", "Expect gzip-ed WARC content (files should end in .warc.gz)."),
                new FlaggedOption("bufferSize", JSAP.INTSIZE_PARSER, DEFAULT_BUFFER_SIZE, JSAP.NOT_REQUIRED, 'b', "buffer-size", "The size of an I/O buffer."),
                new FlaggedOption("spamFile", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 's', "spam-file", "A file containing spam coefficients for documents."),
                new UnflaggedOption("sequence", JSAP.STRING_PARSER, JSAP.REQUIRED, "The filename for the serialized sequence."),
                new UnflaggedOption("basename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, JSAP.GREEDY, "A list of basename files that will be indexed. If missing, a list of files will be read from standard input.")});

        final JSAPResult jsapResult = jsap.parse(args);
        if (jsap.messagePrinted()) System.exit(1);

        final DocumentFactory factory = PropertyBasedDocumentFactory.getInstance(jsapResult.getClass("factory"), jsapResult.getStringArray("property"));
        final boolean isGZipped = jsapResult.getBoolean("gzip");

        String[] file = jsapResult.getStringArray("basename");
        if (file.length == 0) file = IOUtils.readLines(System.in).toArray(new String[0]);
        if (file.length == 0) LOGGER.warn("Empty fileset");
        String spamFile = jsapResult.userSpecified("spamFile") ? jsapResult.getString("spamFile") : null;

        BinIO.storeObject(new WarcDocumentSequence_NYU(file, factory, isGZipped, jsapResult.getInt("bufferSize"), spamFile), jsapResult.getString("sequence"));
    }

    public DocumentFactory factory() {
        return factory;
    }

    protected Document getCurrentDocument(WarcRecord record) throws IOException {

        HttpResponseWarcRecord httpResponse = (HttpResponseWarcRecord) record;
        String guessedCharset = "ISO-8859-1";

        final HttpEntity entity = httpResponse.getEntity();

        // Try to guess using headers
        final Header contentTypeHeader = entity.getContentType();
        if (contentTypeHeader != null) {
            final String headerCharset = HTMLParser.getCharsetNameFromHeader(contentTypeHeader.getValue());
            if (headerCharset != null) guessedCharset = headerCharset;
        }

        final InputStream contentStream = entity.getContent();

		/* Note that the bubing-guessed-charset header and the header guessed by inspecting
            the entity content are complementary. The first is supposed to appear when parsing
			a store, the second while crawling. They should be aligned. This is a bit tricky,
			but we want to avoid the dependency on "rewindable" streams while parsing. */

        final Header bubingGuessedCharsetHeader = httpResponse.getWarcHeader(Name.BUBING_GUESSED_CHARSET);
        if (bubingGuessedCharsetHeader != null) guessedCharset = bubingGuessedCharsetHeader.getValue();
        else {
            if (contentStream instanceof InspectableFileCachedInputStream) {
                final InspectableFileCachedInputStream inspectableStream = (InspectableFileCachedInputStream) contentStream;
                final String metaCharset = HTMLParser.getCharsetName(inspectableStream.buffer, inspectableStream.inspectable);
                if (metaCharset != null) guessedCharset = metaCharset;
            }
        }

        final Reference2ObjectMap<Enum<?>, Object> metadata = new Reference2ObjectOpenHashMap<Enum<?>, Object>();
        metadata.put(PropertyBasedDocumentFactory.MetadataKeys.ENCODING, guessedCharset);

        final Header trecId = httpResponse.getWarcHeaders().getFirstHeader("WARC-TREC-ID");
        if (trecId != null) metadata.put(PropertyBasedDocumentFactory.MetadataKeys.TITLE, trecId.getValue());

        metadata.put(PropertyBasedDocumentFactory.MetadataKeys.URI, httpResponse.getWarcTargetURI().toString());
        if (contentTypeHeader != null)
            metadata.put(PropertyBasedDocumentFactory.MetadataKeys.MIMETYPE, contentTypeHeader.getValue());

        return factory.getDocument(entity.getContent(), metadata);
    }

    public DocumentIterator iterator() throws IOException {

        return new AbstractDocumentIterator() {
            private InputStream currentStream;
            private int n;
            private WarcReader reader;

            @SuppressWarnings("resource")
            public Document nextDocument() throws IOException {
                for (; ; ) {
                    if (currentStream == null) {
                        if (n == warcFile.length) return null;
                        currentStream =
                                useGzip ? new GZIPInputStream(new FileInputStream(warcFile[n++]), bufferSize)
                                        : new FastBufferedInputStream(new FileInputStream(warcFile[n++]), bufferSize);
                        reader = new UncompressedWarcReader_NYU(currentStream);
                    }

                    for (; ; ) {
                        WarcRecord record = null;
                        try {
                            record = reader.read();
                        } catch (Exception ignore) {
                            LOGGER.error("Unexpected exception reading WARC file", ignore);
                            continue;
                        }


                        if (record == null) {
                            currentStream.close();
                            currentStream = null;
                            break;
                        }

                        if (record.getWarcType() == WarcRecord.Type.RESPONSE) {
                            final Header trecId = record.getWarcHeaders().getFirstHeader("WARC-TREC-ID");
                            if (trecId != null && spamDocuments.contains(trecId.getValue())) continue;
                            return getCurrentDocument(record);
                        }
                    }
                }
            }
        };
    }
}
