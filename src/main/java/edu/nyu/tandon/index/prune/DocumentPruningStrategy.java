/*
*
* Juan Rodriguez
*
* create a document strategy for pruning
*
 */
package edu.nyu.tandon.index.prune;

import com.martiansoftware.jsap.*;
import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.index.cluster.DocumentalClusteringStrategy;
import it.unimi.di.big.mg4j.index.cluster.DocumentalPartitioningStrategy;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.util.Properties;
import org.apache.commons.configuration.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;

public class DocumentPruningStrategy implements DocumentalPartitioningStrategy, DocumentalClusteringStrategy, Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(DocumentPruningStrategy.class);

    private static final long serialVersionUID = 0L;
    /**
     * The (cached) number of segments.
     */
    private static final int k = 2;
    public final Long2LongOpenHashMap documents_Global;
    public final Long2LongOpenHashMap documents_Local;

    /**
     * Creates a pruned strategy with the given lists
     */

    public DocumentPruningStrategy(String baseline, String strategy,
                                   Long2LongOpenHashMap docs,
                                   Long2LongOpenHashMap localDocs)
            throws IOException {

        if (docs.size() == 0) throw new IllegalArgumentException("Empty prune list");

        this.documents_Global = docs.clone();
        this.documents_Local = localDocs.clone();

        // create the document titles for local index (local doc IDs)
        ArrayList<String> titles = new ArrayList<String>(documents_Global.size());
        BufferedReader Titles = new BufferedReader(new InputStreamReader(new FileInputStream(baseline), Charset.forName("UTF-8")));
        String line;
        while ((line = Titles.readLine()) != null)
            titles.add(line);
        Titles.close();

        BufferedWriter newTitles = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(strategy +".titles"), Charset.forName("UTF-8")));
        for (long j=0; j<documents_Local.size(); j++) {
            newTitles.write(titles.get((int)documents_Local.get(j)));
            if (j < documents_Local.size()-1) newTitles.newLine();
        }
        newTitles.close();
        titles.clear();
    }

    public static void main(final String[] arg) throws JSAPException, IOException, ConfigurationException, SecurityException,
            URISyntaxException, ClassNotFoundException, InstantiationException, IllegalAccessException,
            InvocationTargetException, NoSuchMethodException {

        final SimpleJSAP jsap = new SimpleJSAP(DocumentPruningStrategy.class.getName(), "Builds a documental partitioning strategy based on a prune list.",
                new Parameter[]{
                        new FlaggedOption("threshold", JSAP.DOUBLE_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 't', "threshold", "Prune threshold for the index (may be specified several times).").setAllowMultipleDeclarations(true),
                        new FlaggedOption("pruningList", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'p', "pruningList", "A file of newline-separated, UTF-8 sorted postings"),
                        new FlaggedOption("strategy", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 's', "The filename for the strategy."),
                        new FlaggedOption("titles", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'T', "The filename for the source titles."),
                        new UnflaggedOption("basename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of the index.")
                });

        JSAPResult jsapResult = jsap.parse(arg);
        if (jsap.messagePrinted()) return;

        final Index index = Index.getInstance(jsapResult.getString("basename"), false, true);

        double[] t_list = jsapResult.getDoubleArray("threshold");
        if (t_list.length == 0)
            throw new IllegalArgumentException("You need to specify at least one ethreshold level.");
        Arrays.sort(t_list);
        double[] threshold = new double[t_list.length];
        boolean[] strategies = new boolean[t_list.length];

        // compute size thresholds based on postings
        for (int i = 0; i < t_list.length; i++) {
            threshold[i] = Math.ceil(((double) index.numberOfPostings) * t_list[i]);
            strategies[i] = true;
        }

        final Long2LongOpenHashMap documentsGlobal = new Long2LongOpenHashMap();
        final Long2LongOpenHashMap documentsLocal = new Long2LongOpenHashMap();

        documentsGlobal.defaultReturnValue(-1);
        LOGGER.info("Generating prunning strategy for " + jsapResult.getString("basename"));

        // read the prune list up to 50%
        BufferedReader prunelist = new BufferedReader(new InputStreamReader(new FileInputStream(jsapResult.getString("pruningList")), Charset.forName("UTF-8")));
        double n = 0;
        String line;
        int j = 0;
        long totPostings = 0;
        while ((line = prunelist.readLine()) != null) {

            String[] tokens = line.split(",");
            if (tokens.length < 2) continue;

            long doc = Long.parseLong(tokens[0]);
            long localDoc = documentsGlobal.get(doc);
            if (localDoc == -1) {
                // assiggn local ID
                localDoc = documentsGlobal.size();
                documentsGlobal.put(doc, localDoc);
                documentsLocal.put(localDoc, doc);
            }

            // compute increase in postings
            totPostings += index.sizes.get(doc);
            n = totPostings;

            // dispatch intermediate strategies if we reached their thresholds
            for (int i = 0; i < t_list.length - 1; i++) {
                if (strategies[i] && n >= threshold[i]) {
                    j++;
                    strategies[i] = false;
                    String level = String.format("%02d", (int) (t_list[i] * 100));
                    BinIO.storeObject(new DocumentPruningStrategy(jsapResult.getString("titles"), jsapResult.getString("strategy") + "-" + level, documentsGlobal, documentsLocal), jsapResult.getString("strategy") + "-" + String.format("%02d", (int) (t_list[i] * 100)) + ".strategy");
                    LOGGER.info(String.valueOf(t_list[i]) + " strategy serialized : " + String.valueOf(documentsGlobal.size()) + " documents, " + String.valueOf((int) Math.ceil(n / 1000000.0)) + "M postings");
                }
            }
            if (n++ >= threshold[threshold.length - 1]) break;

            if ((n % 10000000.0) == 0.0)
                LOGGER.debug(jsapResult.getString("pruneList") + "... " + (int) Math.ceil(n / 1000000.0) + "M");
        }
        if (j >= t_list.length) j--;
        prunelist.close();

        // ran out of input; dump last one
        String level = String.format("%02d", (int) (t_list[j] * 100));
        BinIO.storeObject(new DocumentPruningStrategy(jsapResult.getString("titles"), jsapResult.getString("strategy") + "-" + level, documentsGlobal, documentsLocal), jsapResult.getString("strategy") + "-" + String.format("%02d", (int) (t_list[j] * 100)) + ".strategy");
        LOGGER.info(String.valueOf(t_list[j]) + " strategy serialized : " + String.valueOf(documentsGlobal.size()) + " documents, " + String.valueOf((int) Math.ceil(n / 1000000.0)) + "M postings");
    }

    /* pruned partitioning always creates 2 partitions: 0 and 1. 0 is the pruned index; all others are directed to partition 1 */
    public int numberOfLocalIndices() {
        return 2;
    }

    @Override
    /** return the index of the given document
     * @param globalPointer: global document ID
     */
    /* pruned partitioning always creates 2 partitions: 0 and 1. 0 is the pruned index; all others are directed to partition 1 */
    public int localIndex(final long globalPointer) {
        return (documents_Global.get(globalPointer) == -1) ? 1 : 0;
    }

    /**
     * return the index of the given posting
     *
     * @param term: global term id
     * @param doc:  global document ID
     */
    public int localIndex(final long term, final long doc) {
        throw new RuntimeException("Local Index id from a posting is not supported for document strategies");
    }

    /**
     * return the local document ID
     *
     * @param globalPointer (docID)
     * @return localPointer
     */
    public long localPointer(final long globalPointer) {
        return documents_Global.get(globalPointer);
    }

    public long globalPointer(final int index, final long localPointer) {
        if (index != 0) return -1;
        return documents_Local.get(localPointer);
    }

    /**
     * return the local term ID of a global term ID
     *
     * @param globalTermId
     * @return localId
     */
    public long localTermId(final long globalTermId) {
        throw new RuntimeException("Terms are not supported by this document strategy.");
    }

    public long numberOfDocuments(final int localIndex) {
        return (localIndex == 0) ? documents_Global.size() : 0;
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
