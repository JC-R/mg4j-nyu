package edu.nyu.tandon.tool.xdoc;

import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.io.OutputBitStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

/**
 * Created by juan on 12/2/15.
 */
public class XDOCConcatenate {

    protected final static Logger LOGGER = LoggerFactory.getLogger(XDOCConcatenate.class);
    public static void main(final String[] arg) throws IOException {

        OutputBitStream obs = new OutputBitStream( new FileOutputStream( arg[arg.length-1] )) ;
        long totalDocs = 0;
        long nextID = 0;
        for (int i=0; i<arg.length-1; i++) {
            LOGGER.info( "Reading file: " + arg[i]);
            FileInputStream fis = new FileInputStream( arg[i] );
            InputBitStream ibs = new InputBitStream(fis);
            long docsSeen = 0;
            long id = 0;
            try {
                while (ibs.hasNext()) {
                    id =  ibs.readLongGamma()+totalDocs;
                    obs.writeLongGamma(nextID++);  // id
                    obs.writeGamma(ibs.readGamma());    // len
                    obs.writeGamma(ibs.readGamma());    // unique terms
                    obs.writeInt(ibs.readInt(32),32);    // len
                    docsSeen++;
//                    if (id != nextID) {
//                        LOGGER.debug( "**Mismatch doc ID: " + id + ", " + nextID);
//                    }
//                    nextID++;
                }
            }
            catch (IOException e) {
                ibs.close();
                totalDocs += docsSeen;
                LOGGER.info( "Documents seen: " + totalDocs);
            }
        }
        obs.close();
    }
}
