/*
*
* Juan Rodriguez
*
* extract document features
*
 */
package edu.nyu.tandon.tool;

import com.martiansoftware.jsap.*;
import it.unimi.di.big.mg4j.index.Index;
import it.unimi.dsi.fastutil.io.FastBufferedOutputStream;
import org.apache.commons.configuration.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.nio.charset.Charset;

public class DocumentFeaturesAll {

    private static final Logger LOGGER = LoggerFactory.getLogger(DocumentFeaturesAll.class);

    private static final long serialVersionUID = 0L;

    public static void main(final String[] arg) throws JSAPException, IOException, ConfigurationException, SecurityException,
            URISyntaxException, ClassNotFoundException, InstantiationException, IllegalAccessException,
            InvocationTargetException, NoSuchMethodException {

        final SimpleJSAP jsap = new SimpleJSAP(DocumentFeaturesAll.class.getName(), "Extracts documental features.",
                new Parameter[]{
                        new FlaggedOption("output", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'o', "The output filename ."),
                        new UnflaggedOption("basename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of the index.")
                });

        JSAPResult jsapResult = jsap.parse(arg);
        if (jsap.messagePrinted()) return;

        final Index index = Index.getInstance(jsapResult.getString("basename"), false, true);

        // redirect output
        PrintStream output = (jsapResult.userSpecified("output")) ?
                new PrintStream(new FastBufferedOutputStream(new FileOutputStream(jsapResult.getString("output")))) :
                System.out;

        LOGGER.info("Reading document features for " + jsapResult.getString("basename"));

        for (long n = 0; n<index.numberOfDocuments; n++) {
            long size = index.sizes.get(n);
            if (size != -1) {
                output.printf("%d,%d\n", n, size);
            }
            if ((n % 1000000) == 0)
                LOGGER.debug(Math.ceil(n / 1000000) + "M");
        }

    }
}

