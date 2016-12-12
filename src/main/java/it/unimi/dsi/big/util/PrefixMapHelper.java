package it.unimi.dsi.big.util;

import it.unimi.dsi.lang.MutableString;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class PrefixMapHelper {

    public static String getTerm(ImmutableExternalPrefixMap prefixMap, int index) {
        MutableString s = new MutableString();
        prefixMap.getTerm(index, s);
        return s.toString();
    }
}
