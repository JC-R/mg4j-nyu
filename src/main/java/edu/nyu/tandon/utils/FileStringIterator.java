package edu.nyu.tandon.utils;

import java.io.*;
import java.util.Iterator;
import java.util.function.Consumer;

/**
 * @author michal.siedlaczek@nyu.edu
 */
public class FileStringIterator implements Iterator<String>, AutoCloseable {

    private BufferedReader reader;
    private String next;

    public FileStringIterator(String filename) throws IOException {
        reader = new BufferedReader(new FileReader(filename));
        loadNext();
    }

    private void loadNext() throws IOException {
        next = reader.readLine();
    }

    @Override
    public boolean hasNext() {
        return next != null;
    }

    @Override
    public String next() {
        String r = next;
        try {
            loadNext();
        } catch (IOException e) {
            r = null;
        }
        return r;
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }
}
