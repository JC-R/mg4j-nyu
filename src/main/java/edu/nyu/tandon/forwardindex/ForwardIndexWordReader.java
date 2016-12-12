package edu.nyu.tandon.forwardindex;

import it.unimi.dsi.io.WordReader;
import it.unimi.dsi.lang.MutableString;

import java.io.IOException;
import java.io.Reader;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class ForwardIndexWordReader implements WordReader {

    protected Reader reader;

    @Override
    public boolean next(MutableString mutableString, MutableString mutableString1) throws IOException {
        return false;
    }

    @Override
    public WordReader setReader(Reader reader) {
        this.reader = reader;
        return this;
    }

    @Override
    public WordReader copy() {
        return new ForwardIndexWordReader();
    }
}
