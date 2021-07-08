package edu.nyu.tandon.tool;

import com.martiansoftware.jsap.*;
import it.unimi.di.big.mg4j.document.Document;
import it.unimi.di.big.mg4j.document.DocumentIterator;
import it.unimi.di.big.mg4j.document.DocumentSequence;
import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.index.IndexReader;
import it.unimi.dsi.fastutil.io.BinIO;
import org.apache.commons.configuration.ConfigurationException;

import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class ExtractTitles {

    public static void main(String[] args) throws JSAPException, IOException, ClassNotFoundException, IllegalAccessException, InvocationTargetException, InstantiationException, NoSuchMethodException, URISyntaxException, ConfigurationException {

        SimpleJSAP jsap = new SimpleJSAP(ExtractTitles.class.getName(),
                "Loads indices relative to a collection, possibly loads the collection, and answers to queries.",
                new Parameter[]{
                        new UnflaggedOption("sequence", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.GREEDY, "Document sequence of the corpus.")
                });

        final JSAPResult jsapResult = jsap.parse(args);
        if (jsap.messagePrinted()) return;

        DocumentSequence sequence = (DocumentSequence)BinIO.loadObject(jsapResult.getString("sequence"));
        try (DocumentIterator iterator = sequence.iterator()) {
            Document document;
            while ((document = iterator.nextDocument()) != null) {
                System.out.println(document.title());
            }
        }

    }

}
