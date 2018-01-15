package edu.nyu.tandon.tool;

import com.martiansoftware.jsap.*;
import it.unimi.di.big.mg4j.document.Document;
import it.unimi.di.big.mg4j.document.DocumentIterator;
import it.unimi.di.big.mg4j.document.DocumentSequence;
import it.unimi.di.big.mg4j.index.TermProcessor;
import it.unimi.di.big.mg4j.index.snowball.EnglishStemmer;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.io.FastBufferedReader;
import it.unimi.dsi.lang.MutableString;
import org.apache.commons.configuration.ConfigurationException;

import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class ExtractStemmed {

    DocumentSequence collection;
    String output;

    public ExtractStemmed(String collection, String output) throws IOException, ClassNotFoundException {
        this.collection = (DocumentSequence) BinIO.loadObject(collection);
        this.output = output;
    }

    public void run() throws IOException {
        DocumentIterator documentIterator = collection.iterator();
        Document document;
        TermProcessor termProcessor = new EnglishStemmer();
        try (FileWriter fileWriter = new FileWriter(output);
             FileWriter featuresWriter = new FileWriter(output + ".features")) {
            while ((document = documentIterator.nextDocument()) != null) {

                fileWriter.append(document.title());
                fileWriter.append(" ");
                Long documentSize = 0L;
                FastBufferedReader reader = (FastBufferedReader) document.content(0);
                MutableString word = new MutableString();
                MutableString nonword = new MutableString();
                while (reader.next(word, nonword)) {
                    termProcessor.processTerm(word);
                    fileWriter.append(word);
                    fileWriter.append(" ");
                    documentSize++;
                }
                fileWriter.append("\n");


                String featureLine = String.format("%s\t%s\t%d\n",
                        document.title(),
                        document.uri(),
                        documentSize - 1);
                featuresWriter.append(featureLine);
            }
        }
    }

    public static void main(String[] args) throws IllegalAccessException, URISyntaxException, IOException, InstantiationException, NoSuchMethodException, ConfigurationException, InvocationTargetException, ClassNotFoundException, JSAPException {

        SimpleJSAP jsap = new SimpleJSAP(ExtractStemmed.class.getName(), "",
                new Parameter[]{
                        new UnflaggedOption("collection", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The collection."),
                        new UnflaggedOption("output", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The output file."),
                });

        JSAPResult jsapResult = jsap.parse(args);
        if (jsap.messagePrinted()) return;

        String collection = jsapResult.getString("collection");
        String output= jsapResult.getString("output");

        ExtractStemmed e = new ExtractStemmed(collection, output);
        e.run();
    }

}
