package edu.nyu.tandon.tool;

import com.martiansoftware.jsap.*;
import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.index.IndexReader;
import org.apache.commons.configuration.ConfigurationException;

import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class ExtractTermFrequencies {

    public static void main(String[] args) throws JSAPException, IOException, ClassNotFoundException, IllegalAccessException, InvocationTargetException, InstantiationException, NoSuchMethodException, URISyntaxException, ConfigurationException {

        SimpleJSAP jsap = new SimpleJSAP(ExtractTermFrequencies.class.getName(),
                "Loads indices relative to a collection, possibly loads the collection, and answers to queries.",
                new Parameter[]{
                        new UnflaggedOption("basename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.GREEDY, "Index basename.")
                });

        final JSAPResult jsapResult = jsap.parse(args);
        if (jsap.messagePrinted()) return;

        Index index = Index.getInstance(jsapResult.getString("basename"));
        IndexReader indexReader = index.getReader();

        try (FileWriter writer = new FileWriter(jsapResult.getString("basename") + ".frequencylist")) {
            for (long i = 0; i < index.numberOfTerms; i++) {
                writer.write(String.valueOf(indexReader.documents(i).frequency()) + "\n");
            }
        }
        indexReader.close();

    }

}
