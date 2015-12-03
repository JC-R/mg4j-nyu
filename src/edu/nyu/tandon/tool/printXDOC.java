package edu.nyu.tandon.tool;

import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.io.OutputBitStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

/**
 * Created by juan on 12/2/15.
 */
public class printXDOC {

    protected final static Logger LOGGER = LoggerFactory.getLogger(printXDOC.class);

    public static void main(final String[] arg) throws IOException {

        long docid = 0;
        int pos = 0;
        int terms = 0;
        float xdoc = 0;

        FileInputStream fis = new FileInputStream( arg[0] );
        InputBitStream ibs = new InputBitStream(fis);
        try {
            while (ibs.hasNext()) {

                docid = ibs.readLongGamma();
                pos = ibs.readGamma(); //size
                terms = ibs.readGamma(); // terms
                xdoc = Float.intBitsToFloat(ibs.readInt(32)); // xdoc
                System.out.printf("%d,%d,%d,%e\n", docid, pos, terms, xdoc);
            }
        }
        catch (IOException e) {
            ibs.close();
        }
    }
}
