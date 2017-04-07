package edu.nyu.tandon.index.prune;

import com.martiansoftware.jsap.*;
import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.index.cluster.DocumentalClusteringStrategy;
import it.unimi.di.big.mg4j.index.cluster.DocumentalPartitioningStrategy;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.util.Properties;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.tools.read.SimpleReadSupport;
import org.apache.parquet.tools.read.SimpleRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;

class InputPosting {
	public int termID;
	public int docID;

	public InputPosting(int termID, int docID) {
		this.termID = termID;
		this.docID = docID;
	}
}

public class PostingStrategy implements DocumentalPartitioningStrategy, DocumentalClusteringStrategy, Serializable {

	private static final Logger LOGGER = LoggerFactory.getLogger(PostingStrategy.class);

	private static final long serialVersionUID = 0L;
	/**
	 * The (cached) number of segments.
	 */
	private static final int k = 2;

//    public final Long2ObjectOpenHashMap<LongOpenHashSet> postings_Global;
//    public final Long2LongOpenHashMap documents_Global;
//    public final Long2LongOpenHashMap terms_Global;
//    public final Long2LongOpenHashMap documents_Local;

	// sets implemented as ints -> limit # terms and #docs to ~ 2 billion
	public Long2ObjectOpenHashMap<LongArrayList> postings_Global;
	public Long2LongOpenHashMap documents_Global;
	public Long2LongOpenHashMap documents_Local;

	private static BufferedReader prunelist;
	private static ParquetReader<SimpleRecord> reader;
	private static boolean parquet = false;

	/**
	 * Creates a pruned strategy with the given lists
	 */
	public PostingStrategy(String baseline, String strategy,
	                       final Long2ObjectOpenHashMap<LongArrayList> postings,
	                       Long2LongOpenHashMap docs,
	                       Long2LongOpenHashMap localDocs) throws IOException {

		if (postings.size() == 0 || docs.size() == 0) throw new IllegalArgumentException("Empty prune list");

		// the strategy lists;
		this.postings_Global = postings;
		this.documents_Global = docs;
		this.documents_Local = localDocs;

		// create the document titles for local index (local doc IDs)
		ArrayList<String> titles = new ArrayList<String>(documents_Global.size());
		BufferedReader Titles = new BufferedReader(new InputStreamReader(new FileInputStream(baseline), Charset.forName("UTF-8")));
		String line;
		while ((line = Titles.readLine()) != null)
			titles.add(line);
		Titles.close();

		BufferedWriter newTitles = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(strategy + ".titles"), Charset.forName("UTF-8")));
		for (int j = 0; j < documents_Local.size(); j++) {
			newTitles.write(titles.get((int) documents_Local.get(j)));
			if (j < documents_Local.size() - 1) newTitles.newLine();
		}
		newTitles.close();
		titles.clear();
	}

	private static void wrapReader(ParquetReader<SimpleRecord> r) {
		reader = r;
		parquet = true;
	}

	private static void wrapReader(BufferedReader r) {
		prunelist = r;
		parquet = false;
	}


	private static int termIDCol = -1;
	private static int docIDCol = -1;

	// read posting ID from input file;
	//   fields are static; termID @ position 0, docID @ pos 1
	private static boolean nextPosting(InputPosting p) {
		try {
			if (parquet) {
				SimpleRecord value = reader.read();
				if (value == null) {
					reader.close();
					return false;
				}
//                // see if we have the column indeces yet
//                if (termIDCol == -1) {
//                    for (int j=0; j<value.getValues().size(); j++) {
//                        if (value.getValues().get()) break;
//                    }
//                }
				p.termID = (int) value.getValues().get(0).getValue();
				p.docID = (int) value.getValues().get(1).getValue();
				value = null;
				return true;
			}

			// ascii comma delimited term,doc,....
			else {
				String line;
				while ((line = prunelist.readLine()) != null) {
					String[] tokens = line.split(",");
					if (tokens.length < 2) continue;
					p.termID = Integer.parseInt(tokens[0]);
					p.docID = Integer.parseInt(tokens[1]);
					return true;
				}
				return false;
			}
		} catch (IOException e) {
			return false;
		}
	}

	public static void main(final String[] arg) throws JSAPException, IOException, ConfigurationException, SecurityException,
			URISyntaxException, ClassNotFoundException, InstantiationException, IllegalAccessException,
			InvocationTargetException, NoSuchMethodException {

		final SimpleJSAP jsap = new SimpleJSAP(PostingStrategy.class.getName(), "Builds a documental partitioning strategy based on a prune list.",
				new Parameter[]{
						new FlaggedOption("threshold", JSAP.DOUBLE_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 't', "threshold", "Prune threshold for the index (may be specified several times).").setAllowMultipleDeclarations(true),
						new FlaggedOption("pruningList", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'p', "pruningList", "A file with the sorted postings to use as prune criteria"),
						new Switch("parquet", 'P', "parquet", "pruning input list is in parquet format"),
						new FlaggedOption("strategy", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 's', "The filename for the strategy."),
						new FlaggedOption("titles", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'T', "The filename for the source titles."),
						new UnflaggedOption("basename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of the index.")
				});

		JSAPResult jsapResult = jsap.parse(arg);
		if (jsap.messagePrinted()) return;

		// open main index
		final Index index = Index.getInstance(jsapResult.getString("basename"));
		LOGGER.info("Generating prunning strategy for " + jsapResult.getString("basename"));

		// collect pruning thresholds
		double[] t_list = jsapResult.getDoubleArray("threshold");
		if (t_list.length == 0)
			throw new IllegalArgumentException("You need to specify at least one strategy threshold.");
		Arrays.sort(t_list);
		double[] threshold = new double[t_list.length];
		boolean[] strategies = new boolean[t_list.length];
		for (int i = 0; i < t_list.length; i++) {
			threshold[i] = Math.ceil(((double) index.numberOfPostings) * t_list[i]);
			strategies[i] = true;
		}

		// data elements we ned to track
		final Long2ObjectOpenHashMap<LongArrayList> postings = new Long2ObjectOpenHashMap<LongArrayList>();
		postings.defaultReturnValue(null);

		final Long2LongOpenHashMap documentsGlobal = new Long2LongOpenHashMap();
		documentsGlobal.defaultReturnValue(-1);

		final Long2LongOpenHashMap documentsLocal = new Long2LongOpenHashMap();
		documentsLocal.defaultReturnValue(-1);

		// parquet or ascii input list? Assumed to be in decreasing order
		String input = jsapResult.getString("pruningList");
		if (jsapResult.userSpecified("parquet"))
			wrapReader(new ParquetReader(new Path(input), new SimpleReadSupport()));
		else
			wrapReader(new BufferedReader(new InputStreamReader(new FileInputStream(input), Charset.forName("UTF-8"))));

		long totPostings = 0;
		double n = 0;
		int j = 0;

		LongArrayList termList = null;

		InputPosting p = new InputPosting(-1, -1);
		while (nextPosting(p)) {

			// never seen term?
			if ((termList = postings.get(p.termID)) == null) {
				termList = postings.put(p.termID, new LongArrayList());
			}
			termList.add(p.docID);                      // add to term list
			totPostings++;

			// never seen document
			if (!documentsGlobal.containsKey(p.docID)) {
				int localDoc = documentsGlobal.size();  // next local docID
				documentsGlobal.put(p.docID, localDoc);
				documentsLocal.put(localDoc, p.docID);
			}

			// dispatch intermediate strategies if we reached their thresholds
			for (int i = 0; i < t_list.length - 1; i++) {
				if (strategies[i] && n >= threshold[i]) {
					j++;
					strategies[i] = false;
					String level = String.format("%02d", (int) (t_list[i] * 100));
					PostingStrategy ps = new PostingStrategy(jsapResult.getString("titles"), jsapResult.getString("strategy") + "-" + level, postings, documentsGlobal, documentsLocal);
					BinIO.storeObject(ps, jsapResult.getString("strategy") + "-" + String.format("%02d", (int) (t_list[i] * 100)) + ".strategy");
					ps = null;
					LOGGER.info(String.valueOf(t_list[i]) + " strategy serialized : " + String.valueOf(documentsGlobal.size()) + " documents, " + String.valueOf((int) Math.ceil(n / 1000000.0)) + "M postings");
				}
			}
			if (n++ >= threshold[threshold.length - 1]) break;

			if ((n % 10000000.0) == 0.0)
				LOGGER.info(jsapResult.getString("pruneList") + "... " + (int) Math.ceil(n / 1000000.0) + "M");
		}
		if (j >= t_list.length) j--;

		if (parquet) reader.close();
		else prunelist.close();

		// ran out of input; dump last set
		String level = String.format("%02d", (int) (t_list[j] * 100));
		BinIO.storeObject(new PostingStrategy(jsapResult.getString("titles"), jsapResult.getString("strategy") + "-" + level, postings, documentsGlobal, documentsLocal), jsapResult.getString("strategy") + "-" + String.format("%02d", (int) (t_list[j] * 100)) + ".strategy");
		LOGGER.info(String.valueOf(t_list[j]) + " strategy serialized : " + String.valueOf(documentsGlobal.size()) + " documents, " + String.valueOf((int) Math.ceil(n / 1000000.0)) + "M postings");

	}

	/* pruned partitioning always creates 2 partitions: 0 and 1. 0 is the pruned one; all others are directed to partition 1 */
	public int numberOfLocalIndices() {
		return 2;
	}

	@Override
	/** return the index of the given document
	 * @param globalPointer: global document ID
	 */
	public int localIndex(final long globalPointer) {
		return (documents_Global.containsKey((int) globalPointer)) ? 0 : 1;
	}

	/**
	 * return the index of the given posting
	 *
	 * @param term: global term id
	 * @param doc:  global document ID
	 */
	public int localIndex(final long term, final long doc) {
		LongArrayList s;
		if ((s = postings_Global.get((int) term)) == null) return 1;
		return (s.contains((int) doc)) ? 0 : 1;
	}

	/**
	 * return the local document ID
	 *
	 * @param globalPointer (docID)
	 * @return localPointer
	 */
	public long localPointer(final long globalPointer) {
		return documents_Global.get((int) globalPointer);
	}

	public long globalPointer(final int index, final long localPointer) {
		if (index != 0) return -1;
		return documents_Local.get((int) localPointer);
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
