package edu.nyu.tandon.tool;
		/*
		 *
		 *  This library is free software; you can redistribute it and/or modify it
		 *  under the terms of the GNU Lesser General Public License as published by the Free
		 *  Software Foundation; either version 3 of the License, or (at your option)
		 *  any later version.
		 *
		 *  This library is distributed in the hope that it will be useful, but
		 *  WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
		 *  or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
		 *  for more details.
		 *
		 *  You should have received a copy of the GNU Lesser General Public License
		 *  along with this program; if not, see <http://www.gnu.org/licenses/>.
		 *
		 */

import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.index.IndexIterator;
import it.unimi.di.big.mg4j.index.IndexReader;
import it.unimi.di.big.mg4j.search.DocumentIterator;
import it.unimi.di.big.mg4j.search.score.BM25Scorer;
import it.unimi.di.big.mg4j.search.score.Scorer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.ByteBuffer;

/**
 * Iterate through the entire index and prints out the posting IDs: termID, docID
 *
 * @author Juan Rodriguez
 * @since 1.0
 */


public class IndexPostingID {


	public static void main(String arg[]) throws Exception {

		long document = 0;
		long i = 0;
		long tID = -1;

		if (arg.length < 2) {
			System.out.println("Missing argument: IndexFeatures <index> <output_file>");
			System.exit(-1);
		}

		String outputFile = arg[1];
		FileOutputStream fos = new FileOutputStream(outputFile);
		DataOutputStream dos = new DataOutputStream(fos);

		Logger LOGGER = LoggerFactory.getLogger(IndexPostingID.class);

		/** First we open our index. The booleans tell that we want random access to
		 * the inverted lists, and we are going are no using  */
		final Index text = Index.getInstance(arg[0], true, false);
		final IndexReader reader = text.getReader();
		IndexIterator iterator;

		// for each term in the index, extract the term features: termID,docID
		while ((iterator = reader.nextIterator()) != null) {
			tID = iterator.termNumber();
			while ((document = iterator.nextDocument()) != DocumentIterator.END_OF_LIST) {
				i++;
				dos.writeLong(tID);
				dos.writeInt((int) document);
			}
		}
		dos.close();
		fos.close();
		System.out.printf("%s - %d postings written\n", arg[1], i);
	}
}
