package edu.nyu.tandon.utils;

import it.unimi.di.big.mg4j.tool.Scan;
import it.unimi.dsi.fastutil.objects.Object2FloatOpenHashMap;
import it.unimi.dsi.lang.MutableString;

import java.io.BufferedReader;
import java.io.FileReader;

/**
 * Created by juan on 11/29/15.
 */
public class languageModel {

    public Object2FloatOpenHashMap<MutableString> Unigrams;

    public languageModel() {
    }

    public languageModel(String fileName) throws Exception {
        Unigrams = new Object2FloatOpenHashMap<MutableString>(Scan.INITIAL_TERM_MAP_SIZE);
        loadArpa(fileName);
    }

    /**
     * // This is an ARPA format reader, tab delimited
     * Loads the language model unigram file: first entry is assumed to be P(UNK)
     * Ignore line with < 3 tokens
     * First line is P(UNK), given by empty or
     */

    public void loadArpa(String fileName) throws Exception {

        BufferedReader uIn = new BufferedReader(new FileReader(fileName));
        String line;
        String pUnknown = null;
        String[] tokens;
        float p;

        // find start of unigrams
        while ((line = uIn.readLine()) != null) {
            if (line.compareTo("\\1-grams:") == 0)
                break;
        }

        // P(unk)
        while ((line = uIn.readLine()) != null) {
            tokens = line.split("\t");
            if (tokens.length < 3) continue;
            if (tokens[1].compareTo("") == 0 || tokens[1].compareTo("<UNK>") == 0) {
                pUnknown = tokens[0];
                Unigrams.defaultReturnValue((float) Math.pow(10, Float.parseFloat(tokens[0])));   // null key = P(unk)
                break;
            }
        }

        // P(unk)
        while ((line = uIn.readLine()) != null) {
            tokens = line.split("\t");
            if (tokens.length < 3)
                break;
            if (tokens[0].compareTo(pUnknown) == 0)
                continue;
            Unigrams.put(new MutableString(tokens[1]), (float) Math.pow(10, Float.parseFloat(tokens[0])));
        }
        uIn.close();
    }
}
