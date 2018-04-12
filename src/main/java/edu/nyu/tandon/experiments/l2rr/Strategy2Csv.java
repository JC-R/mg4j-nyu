package edu.nyu.tandon.experiments.l2rr;

import com.martiansoftware.jsap.*;
import edu.nyu.tandon.index.cluster.SelectiveDocumentalIndexStrategy;
import edu.nyu.tandon.query.Query;
import it.unimi.dsi.fastutil.io.BinIO;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class Strategy2Csv {


    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws Exception {

        SimpleJSAP jsap = new SimpleJSAP(Query.class.getName(), ".",
                new Parameter[]{
                        new Switch("skipHeader", 'H', "skip-header", "Do not write CSV header."),
                        new UnflaggedOption("strategy", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The clustering strategy."),
                        new UnflaggedOption("documents", JSAP.LONG_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The number of documents.")
                });

        final JSAPResult jsapResult = jsap.parse(args);
        if (jsap.messagePrinted()) return;

        SelectiveDocumentalIndexStrategy strategy = (SelectiveDocumentalIndexStrategy)
                BinIO.loadObject(jsapResult.getString("strategy"));

        if (!jsapResult.userSpecified("skipHeader")) {
            System.out.println("gdocid,ldocid,shard");
        }

        for (long document = 0L; document < jsapResult.getLong("documents"); document++) {
            StringBuilder sb = new StringBuilder();
            sb.append(document)
                    .append(',')
                    .append(strategy.localPointer(document))
                    .append(',')
                    .append(strategy.localIndex(document));
            System.out.println(sb);
        }
    }

}
