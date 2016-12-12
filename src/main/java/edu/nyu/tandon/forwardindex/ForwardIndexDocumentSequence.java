package edu.nyu.tandon.forwardindex;

import edu.nyu.tandon.forwardindex.ForwardIndex.TermMap;
import edu.nyu.tandon.utils.Utils;
import it.unimi.di.big.mg4j.document.Document;
import it.unimi.di.big.mg4j.document.*;
import it.unimi.dsi.big.util.ImmutableExternalPrefixMap;
import it.unimi.dsi.big.util.PrefixMapHelper;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.longs.LongBigArrayBigList;
import it.unimi.dsi.fastutil.objects.Reference2ObjectMap;
import it.unimi.dsi.fastutil.objects.Reference2ObjectOpenHashMap;
import org.apache.commons.configuration.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class ForwardIndexDocumentSequence implements DocumentSequence {

    public static final Logger LOGGER = LoggerFactory.getLogger(ForwardIndexDocumentSequence.class);

    protected static final int DEFAULT_CAPACITY = 10000;

    protected IdentityDocumentFactory factory;
    protected ForwardIndexIterator forwardIndexIterator;
    protected LongBigArrayBigList term2rank;
    protected ImmutableExternalPrefixMap termMap;

    public ForwardIndexDocumentSequence(ForwardIndex forwardIndex, File termMapFile) throws ConfigurationException, IOException, ClassNotFoundException {
        forwardIndexIterator = forwardIndex.getReader().getIterator();
        term2rank = Utils.readMapping(forwardIndex.termMapFile);
        termMap = (ImmutableExternalPrefixMap) BinIO.loadObject(termMapFile);
        factory = new IdentityDocumentFactory();
    }

    @Override
    public DocumentIterator iterator() throws IOException {
        return new DocumentIterator() {
            @Override
            public Document nextDocument() throws IOException {
                Reference2ObjectMap<Enum<?>,Object> metadata = new Reference2ObjectOpenHashMap<>();
                edu.nyu.tandon.forwardindex.Document doc = forwardIndexIterator.next();
                metadata.put(PropertyBasedDocumentFactory.MetadataKeys.TITLE, doc.getMetadata().getTitle());
                metadata.put(PropertyBasedDocumentFactory.MetadataKeys.ENCODING, StandardCharsets.UTF_8.name());
                StringBuilder builder = new StringBuilder();
                for (Long termId : doc) {
                    builder.append(PrefixMapHelper.getTerm(termMap, (int) term2rank.getLong(termId))).append(" ");
                }
                return factory.getDocument(new ByteArrayInputStream(builder.toString().getBytes(StandardCharsets.UTF_8)),
                        metadata);
            }

            @Override
            public void close() throws IOException {
                try {
                    forwardIndexIterator.close();
                } catch (IOException e) {
                    throw e;
                } catch (Exception e) {
                    throw new RuntimeException("unable to close document sequence", e);
                }
            }
        };
    }

    @Override
    public DocumentFactory factory() {
        return factory;
    }

    @Override
    public void close() throws IOException {
        try {
            forwardIndexIterator.close();
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("unable to close document sequence", e);
        }
    }

    @Override
    public void filename(CharSequence charSequence) throws IOException {
    }
}
