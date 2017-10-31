package edu.nyu.tandon.utils;

import java.io.IOException;
import java.util.Iterator;

import static java.lang.Long.parseLong;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class FileAsciiLongIterator implements Iterator<Long>, AutoCloseable {

    FileStringIterator stringIterator;

    public FileAsciiLongIterator(String filename) throws IOException {
        stringIterator = new FileStringIterator(filename);
    }

    @Override
    public void close() throws Exception {
        stringIterator.close();
    }

    @Override
    public boolean hasNext() {
        return stringIterator.hasNext();
    }

    @Override
    public Long next() {
        String line = stringIterator.next();
        return parseLong(line);
    }
}
