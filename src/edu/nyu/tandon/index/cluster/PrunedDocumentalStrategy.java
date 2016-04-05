package edu.nyu.tandon.index.cluster;

import com.martiansoftware.jsap.*;
import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.index.cluster.DocumentalClusteringStrategy;
import it.unimi.di.big.mg4j.index.cluster.DocumentalPartitioningStrategy;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.util.Properties;
import org.apache.commons.configuration.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.util.Arrays;

public class PrunedDocumentalStrategy implements DocumentalPartitioningStrategy, DocumentalClusteringStrategy, Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(PrunedDocumentalStrategy.class);

    private static final long serialVersionUID = 0L;
    /**
     * The (cached) number of segments.
     */
    private static final int k = 2;
    public final Long2ObjectOpenHashMap<LongOpenHashSet> localTerms;
    public final LongOpenHashSet localDocuments;

    /**
     * Creates a pruned strategy with the given postings
     */

    public PrunedDocumentalStrategy(final Long2ObjectOpenHashMap<LongOpenHashSet> terms, LongOpenHashSet docs) {


        if (terms.size() == 0 || docs.size() == 0) throw new IllegalArgumentException("Empty prune list");

        localTerms = terms.clone();
        localDocuments = docs.clone();

    }

    public static void main(final String[] arg) throws JSAPException, IOException, ConfigurationException, SecurityException,
            URISyntaxException, ClassNotFoundException, InstantiationException, IllegalAccessException,
            InvocationTargetException, NoSuchMethodException {

        final SimpleJSAP jsap = new SimpleJSAP(PrunedLexicalStrategy.class.getName(), "Builds a documental partitioning strategy based on a prune list.",
                new Parameter[]{
                        new FlaggedOption("threshold", JSAP.DOUBLE_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 't', "threshold", "Prune threshold for the index (may be specified several times).").setAllowMultipleDeclarations(true),
                        new UnflaggedOption("basename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of the index."),
                        new UnflaggedOption("prunelist", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The ordered postings list"),
                        new UnflaggedOption("strategy", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The filename for the strategy.")
                });

        JSAPResult jsapResult = jsap.parse(arg);
        if (jsap.messagePrinted()) return;

        final Index index = Index.getInstance(jsapResult.getString("basename"));

        double[] t_list = jsapResult.getDoubleArray("threshold");
        if (t_list.length == 0) throw new IllegalArgumentException("You need to specify at least on ethreshold level.");
        Arrays.sort(t_list);
        double[] threshold = new double[t_list.length];
        boolean[] strategies = new boolean[t_list.length];
        for (int i = 0; i < t_list.length; i++) {
            threshold[i] = Math.ceil(((double) index.numberOfPostings) * t_list[i]);
            strategies[i] = true;
        }
        final Long2ObjectOpenHashMap<LongOpenHashSet> postings = new Long2ObjectOpenHashMap<LongOpenHashSet>();
        postings.defaultReturnValue(null);

        final LongOpenHashSet documents = new LongOpenHashSet();

        LOGGER.info("Generating posting prunning strategy for " + jsapResult.getString("basename"));

        // read the prune list up to 50%
        BufferedReader prunelist = new BufferedReader(new InputStreamReader(new FileInputStream(jsapResult.getString("prunelist")), Charset.forName("UTF-8")));
        double n = 0;
        String line;
        while ((line = prunelist.readLine()) != null) {

            String[] tokens = line.split(",");
            if (tokens.length < 2) continue;

            long term = Long.parseLong(tokens[0]);
            long doc = Long.parseLong(tokens[1]);

            if (!postings.containsKey(term)) {
                postings.put(term, new LongOpenHashSet());
            }
            postings.get(term).add(doc);
            if (!documents.contains(doc)) documents.add(doc);

            // dispatch intermediate strategies
            for (int i = 0; i < t_list.length - 1; i++) {
                if (strategies[i] && n >= threshold[i]) {
                    strategies[i] = false;
                    BinIO.storeObject(new PrunedDocumentalStrategy(postings, documents), jsapResult.getString("strategy") + "." + String.valueOf(t_list[i]) + ".strategy");
                    LOGGER.info(String.valueOf(t_list[i]) + " strategy serialized : " + String.valueOf((int) Math.ceil(n / 1000000.0)) + "M postings");
                }
            }

            if (n++ >= threshold[threshold.length - 1]) break;

            if ((n % 10000000.0) == 0.0)
                LOGGER.info(jsapResult.getString("prunelist") + "... " + (int) Math.ceil(n / 1000000.0) + "M");
        }

        prunelist.close();

        // dump last one
        BinIO.storeObject(new PrunedDocumentalStrategy(postings, documents), jsapResult.getString("strategy") + "." + String.valueOf(t_list[t_list.length - 1]) + ".strategy");
        LOGGER.info(String.valueOf(t_list[t_list.length - 1]) + " strategy serialized : " + String.valueOf((int) Math.ceil(n / 1000000.0)) + "M postings");

    }

    public int numberOfLocalIndices() {
        return 2;
    }

    /**
     * return the posting list of a termID
     *
     * @param globalNumber: global termID
     * @return
     */
    public LongOpenHashSet localTermList(final long globalNumber) {
        return localTerms.get(globalNumber);
    }

    @Override
    /** return the index of the given document
     * @param globalPointer: global document ID
     */
    public int localIndex(final long globalPointer) {
        return localDocuments.contains(globalPointer) ? 0 : 1;
    }

    /**
     * return the index of the given posting
     *
     * @param term: term id
     * @param doc:  document ID
     */
    public int localIndex(final long term, final long doc) {
        LongOpenHashSet l = localTerms.get(term);
        if (l == null) return 1;
        return (l.contains(doc)) ? 0 : 1;
    }

    /**
     * return the local document ID
     *
     * @param globalPointer
     * @return
     */
    public long localPointer(final long globalPointer) {
        return 0;
//		return globalPointer - cutPoint[ localIndex( globalPointer ) ];
    }

    public long globalPointer(final int localIndex, final long localPointer) {
//		return localPointer + cutPoint[ localIndex ];
        return 0;
    }

    public long numberOfDocuments(final int localIndex) {

//		return cutPoint[ localIndex + 1 ] - cutPoint[ localIndex ];
        return (localIndex == 0) ? localDocuments.size() : 0;
    }

//	public String toString() {
//		return Arrays.toString( cutPoint );
//	}

    public Properties[] properties() {
//		Properties[] properties = new Properties[ k ];
//		for( int i = 0; i < k; i++ ) {
//			properties[ i ] = new Properties();
//			properties[ i ].addProperty( "pointerfrom", cutPoint[ i ] );
//			properties[ i ].addProperty( "pointerto", cutPoint[ i + 1 ] );
//		}
//		return properties;
        return (null);
    }
}
