package edu.nyu.tandon.tool;

import it.unimi.di.big.mg4j.tool.Scan;
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

//        DataOutputStream Corpus = new DataOutputStream(new FileOutputStream( arg[arg.length-1] ));
        OutputBitStream obs = new OutputBitStream( new FileOutputStream( arg[arg.length-1] )) ;
        long totalDocs = 0;


        for (int i=0; i<arg.length-1; i++) {
            LOGGER.info( "Reading file: " + arg[i]);

            FileInputStream fis = new FileInputStream( arg[i] );
            InputBitStream ibs = new InputBitStream(fis);
            long docsSeen = 0;
            try {
                while (ibs.hasNext()) {
                    obs.writeLongGamma(ibs.readLongGamma()+totalDocs);  // id
                    obs.writeGamma(ibs.readGamma());    // len
                    obs.writeGamma(ibs.readGamma());    // unique terms
                    obs.writeInt(ibs.readInt(32),32);    // len
                    docsSeen++;
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
