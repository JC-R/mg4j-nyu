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

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;

/**
 * Iterate through the entire index and prints out the index features
 // -for each term in the index, prints term features: termID,docID,term-freq,doc-term-freq,bm25 to standard output
 *
 * @author Juan Rodriguez
 * @since 1.0
 */


public class IndexFeatures {


    public static void main(String arg[]) throws Exception {

        long document = 0;
        double score = 0;

        Logger LOGGER = LoggerFactory.getLogger(IndexFeatures.class);

        Scorer scorer = new BM25Scorer();

        /** First we open our index. The booleans tell that we want random access to
         * the inverted lists, and we are going to use document sizes (for scoring--see below). */
        final Index text = Index.getInstance(arg[0] + "-text", true, true);
        final IndexReader reader = text.getReader();

        BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(arg[0] + "-text.terms"), Charset.forName("UTF-8")));

        IndexIterator iterator;
        while ((iterator = reader.nextIterator()) != null) {
            // for each term in the index, extract the term features: termID,docID,term-freq,doc-term-freq,bm25
            iterator.term(br.readLine());
            scorer.wrap(iterator);
            while ((document = iterator.nextDocument()) != DocumentIterator.END_OF_LIST) {
                System.out.printf("%d,%d,%d,%d,%f\n", iterator.termNumber(), document, iterator.frequency(), iterator.count(), scorer.score());
            }
        }

    }
}
