package edu.nyu.tandon.forwardindex;

import com.google.common.base.Charsets;
import edu.nyu.tandon.forwardindex.ForwardIndex.TermMap;
import it.unimi.di.big.mg4j.document.*;
import it.unimi.di.big.mg4j.document.Document;
import it.unimi.dsi.fastutil.objects.Reference2ObjectMap;
import it.unimi.dsi.fastutil.objects.Reference2ObjectOpenHashMap;
import it.unimi.dsi.io.WordReader;
import it.unimi.dsi.util.Properties;
import org.apache.commons.configuration.ConfigurationException;

import java.io.*;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class ForwardIndexDocumentSequence implements DocumentSequence {

    protected static final int DEFAULT_CAPACITY = 10000;

    protected IdentityDocumentFactory factory;
    protected ForwardIndexIterator forwardIndexIterator;
    protected TermMap termMap;

    public ForwardIndexDocumentSequence(ForwardIndex forwardIndex) throws ConfigurationException, IOException {
        forwardIndexIterator = forwardIndex.getReader().getIterator();
        termMap = forwardIndex.getTermMap(DEFAULT_CAPACITY);
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
                metadata.put(PropertyBasedDocumentFactory.MetadataKeys.ENCODING, Charsets.UTF_8);
                StringBuilder builder = new StringBuilder();
                for (Long termId : doc) {
                    builder.append(termMap.get(termId)).append(" ");
                }
                return factory.getDocument(new ByteArrayInputStream(builder.toString().getBytes(Charsets.UTF_8)),
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
