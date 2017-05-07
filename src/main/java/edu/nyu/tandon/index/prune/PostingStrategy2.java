package edu.nyu.tandon.index.prune;

import com.martiansoftware.jsap.*;
import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.index.cluster.DocumentalClusteringStrategy;
import it.unimi.di.big.mg4j.index.cluster.DocumentalPartitioningStrategy;
import it.unimi.dsi.fastutil.ints.*;

import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.util.Properties;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.hadoop.fs.Path;

import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.tools.read.SimpleReadSupport;
import org.apache.parquet.tools.read.SimpleRecord;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;

public class PostingStrategy2 implements DocumentalPartitioningStrategy, DocumentalClusteringStrategy, Serializable {

	private static final Logger LOGGER = LoggerFactory.getLogger(PostingStrategy2.class);

	public static final long serialVersionUID = 3L;
	/**
	 * The (cached) number of segments.
	 */
	private static final int k = 2;

	// sets implemented as ints -> limit # terms and #docs to ~ 2 billion
	public Int2ObjectOpenHashMap<IntArrayList> postings_Global = null;
	public IntArrayList documents_Global = null;

	// built on use
	public IntArrayList documents_Local = null;

	public int localDocSize = 0;
	private static BufferedReader prunelist;
	private static ParquetReader<SimpleRecord> reader;
	private static boolean parquet = false;

	/**
	 * create map of terms and docs in pruned list
	 */
	public void initMaps() {

		int d;
		this.documents_Local = new IntArrayList(documents_Global.size());
		for (int j = 0; j < documents_Global.size(); j++)
			documents_Local.add(j, -1);
		for (int j = 0; j < documents_Global.size(); j++) {
			d = documents_Global.getInt(j);
			if (d > -1) {
				this.documents_Local.set(d, j);
				localDocSize++;
			}
		}
	}

	/**
	 * Creates a pruned strategy with the given lists
	 */
	public PostingStrategy2(final Int2ObjectOpenHashMap<IntArrayList> postings,
	                        final IntArrayList globalDocs
	) throws IOException {

		if (postings.size() == 0) throw new IllegalArgumentException("Empty prune list");
		this.documents_Global = globalDocs;
		this.postings_Global = postings;

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
				// see if we have the column indeces yet
				if (termIDCol == -1) {
					for (int j = 0; j < value.getValues().size(); j++) {
						if (value.getValues().get(j).getName() == "termID") {
							termIDCol = j;
							break;
						}
					}
				}
				if (docIDCol == -1) {
					for (int j = 0; j < value.getValues().size(); j++) {
						if (value.getValues().get(j).getName() == "docID") {
							docIDCol = j;
							break;
						}
					}
				}

				p.termID = (int) value.getValues().get(termIDCol).getValue();
				p.docID = (int) value.getValues().get(docIDCol).getValue();
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


		final SimpleJSAP jsap = new SimpleJSAP(PostingStrategy2.class.getName(), "Builds a documental partitioning strategy based on a prune list.",
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

		// strategy titles
		BufferedWriter[] newTitles = new BufferedWriter[t_list.length];

		for (int j = 0; j < t_list.length; j++) {
			String level = String.format("%02d", (int) (t_list[j] * 100));
			newTitles[j] = new BufferedWriter(
					new OutputStreamWriter(
							new FileOutputStream(jsapResult.getString("strategy") + "-" + level + ".titles"),
							Charset.forName("UTF-8")));
		}

		// data elements to track
		final Int2ObjectOpenHashMap<IntArrayList> postings = new Int2ObjectOpenHashMap<IntArrayList>();
		postings.defaultReturnValue(null);

		// assume most documents will make it; use simple array now
		final IntArrayList docs = new IntArrayList((int) index.numberOfDocuments);
		for (int d = 0; d < index.numberOfDocuments; d++) docs.add(d, -1);

		// create the document titles for local index (local doc IDs)
		ArrayList<String> titles = new ArrayList<String>((int) index.numberOfDocuments);
		BufferedReader Titles = new BufferedReader(new InputStreamReader(
				new FileInputStream(jsapResult.getString("titles")),
				Charset.forName("UTF-8")));
		String line;
		while ((line = Titles.readLine()) != null) titles.add(line);
		Titles.close();

		// parquet or ascii input list? Assumed to be in decreasing order
		String input = jsapResult.getString("pruningList");
		if (jsapResult.userSpecified("parquet"))
			wrapReader(new ParquetReader<>(new Path(input), new SimpleReadSupport()));
		else
			wrapReader(new BufferedReader(new InputStreamReader(new FileInputStream(input), Charset.forName("UTF-8"))));

		long totPostings = 0;
		int totDocs = 0;
		double n = 0;
		int j = 0;

		final InputPosting p = new InputPosting(-1, -1);

		while (nextPosting(p)) {

			// never seen term?
			if (!postings.containsKey(p.termID)) postings.put(p.termID, new IntArrayList());

			postings.get(p.termID).add(p.docID);
			totPostings++;

			if (docs.getInt(p.docID) == -1) {

				// new doc
				docs.set(p.docID, totDocs++);

				// dispatch strategy titles
				line = titles.get(p.docID);
				for (int k = 0; k < t_list.length; k++) {
					if (strategies[k]) {
						newTitles[k].write(line);
						newTitles[k].newLine();
					}
				}
			}

			// dispatch intermediate strategies if we reached their thresholds
			for (int i = 0; i < t_list.length - 1; i++) {
				if (strategies[i] && n >= threshold[i]) {

					j++;
					strategies[i] = false;
					newTitles[i].close();
					String level = String.format("%02d", (int) (t_list[i] * 100));
					BinIO.storeObject(new PostingStrategy2(postings, docs), jsapResult.getString("strategy") + "-" + String.format("%02d", (int) (t_list[i] * 100)) + ".strategy");
					LOGGER.info(String.valueOf(t_list[i]) + " strategy serialized : " + String.valueOf(totDocs) + " documents, " + String.valueOf((int) Math.ceil(n / 1000000.0)) + "M postings");
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
		BinIO.storeObject(new PostingStrategy2(postings, docs),
				jsapResult.getString("strategy") + "-" + String.format("%02d", (int) (t_list[j] * 100)) + ".strategy");
		LOGGER.info(String.valueOf(t_list[j]) + " strategy serialized : " + String.valueOf(totDocs) + " documents, " + String.valueOf((int) Math.ceil(n / 1000000.0)) + "M postings");

	}

	/* pruned partitioning always creates 2 partitions: 0 and 1. 0 is the pruned one; all others are directed to partition 1 */
	public int numberOfLocalIndices() {
		return 2;
	}

	@Override
	/** return the index of the given document
	 * @param globalPointer: global document ID
	 */
	public int localIndex(final long globalID) {

		return (documents_Global.getInt((int) globalID) > -1) ? 0 : 1;
	}

	/**
	 * return the index of the given posting
	 *
	 * @param term: global term id
	 * @param doc:  global document ID
	 */
	public int localIndex(final long term, final long doc) {
		if (!postings_Global.containsKey((int) term)) return 1;
		return (postings_Global.get((int) term).contains((int) doc)) ? 0 : 1;
	}

	/**
	 * return the local document ID
	 *
	 * @param globalPointer (docID)
	 * @return localPointer
	 */
	public long localPointer(final long globalPointer) {
		return documents_Global.getInt((int) globalPointer);
	}

	public long globalPointer(final int index, final long localPointer) {
		if (index != 0) return -1;
		if (documents_Local == null) initMaps();
		return documents_Local.getInt((int) localPointer);
	}

	public long numberOfDocuments(final int localIndex) {
		return (localIndex == 0) ? localDocSize : 0;

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
