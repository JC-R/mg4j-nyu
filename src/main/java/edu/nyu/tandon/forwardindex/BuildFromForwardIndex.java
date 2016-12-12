package edu.nyu.tandon.forwardindex;

import com.martiansoftware.jsap.*;
import edu.nyu.tandon.coding.VarByteDecoder;
import it.unimi.di.big.mg4j.tool.IndexBuilder;
import org.apache.commons.configuration.ConfigurationException;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class BuildFromForwardIndex {

    public static void main(String[] args) throws JSAPException, IllegalAccessException, IOException, NoSuchMethodException, ClassNotFoundException, ConfigurationException, InvocationTargetException, URISyntaxException, InstantiationException {

        SimpleJSAP jsap = new SimpleJSAP(BuildFromForwardIndex.class.getName(), "Build an index from a Forward Index structure.",
                new Parameter[]{
                        new FlaggedOption("collection", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'c', "collection", "The collection file."),
                        new FlaggedOption("metadata", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'm', "metadata", "The document metadata file."),
                        new FlaggedOption("term2rank", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 't', "term2rank", "The term to rank mapping."),
                        new FlaggedOption("prefixMap", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'p', "prefixmap", "The term prefix map."),
                        new UnflaggedOption("basename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of the index.")
                });

        final JSAPResult jsapResult = jsap.parse(args);
        if (jsap.messagePrinted()) return;


        IndexBuilder indexBuilder = new IndexBuilder(jsapResult.getString("basename"),
                new ForwardIndexDocumentSequence(new ForwardIndex(
                        jsapResult.getFile("collection"),
                        jsapResult.getFile("metadata"),
                        jsapResult.getFile("terms"),
                        VarByteDecoder.getFlipped()
                ), jsapResult.getFile("prefixMap")));

        indexBuilder.run();

    }

}
