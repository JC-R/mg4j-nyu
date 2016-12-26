package edu.nyu.tandon.document;

import it.unimi.di.big.mg4j.document.*;
import it.unimi.dsi.fastutil.objects.Reference2ObjectMap;
import it.unimi.dsi.fastutil.objects.Reference2ObjectOpenHashMap;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class StringDocumentSequence implements DocumentSequence {

    protected IdentityDocumentFactory factory;
    protected List<String> documents;
    protected Charset charset;

    public StringDocumentSequence(List<String> documents, Charset charset) {
        this.documents = documents;
        this.charset = charset;
        this.factory = new IdentityDocumentFactory();
    }

    public StringDocumentSequence(List<String> documents) {
        this(documents, StandardCharsets.UTF_8);
    }

    @Override
    public DocumentIterator iterator() throws IOException {
        return new DocumentIterator() {

            Iterator<String> it = documents.iterator();

            @Override
            public Document nextDocument() throws IOException {
                if (!it.hasNext()) return null;
                String content = it.next();
                Reference2ObjectMap<Enum<?>,Object> metadata = new Reference2ObjectOpenHashMap<>();
                metadata.put(PropertyBasedDocumentFactory.MetadataKeys.ENCODING, charset.name());
                return factory.getDocument(new ByteArrayInputStream(content.getBytes(charset)), metadata);
            }

            @Override
            public void close() throws IOException {
            }
        };
    }

    @Override
    public DocumentFactory factory() {
        return this.factory;
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public void filename(CharSequence filename) throws IOException {
    }
}
