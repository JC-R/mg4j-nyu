package edu.nyu.tandon.experiments.StaticPruning;
/**
 * Created by juan on 3/20/17.
 */

import java.io.*;
import hex.genmodel.easy.RowData;
import hex.genmodel.easy.EasyPredictModelWrapper;
import hex.genmodel.easy.prediction.*;
import hex.genmodel.MojoModel;

public class h2oPredict {

	public static void main(String[] args) throws Exception {


		EasyPredictModelWrapper model = new EasyPredictModelWrapper(MojoModel.load(args[0]));

		RowData row = new RowData();
		row.put("AGE", "68");
		row.put("RACE", "2");
		row.put("DCAPS", "2");
		row.put("VOL", "0");
		row.put("GLEASON", "6");


		BinomialModelPrediction p = model.predictBinomial(row);
		System.out.println("Has penetrated the prostatic capsule (1=yes; 0=no): " + p.label);
		System.out.print("Class probabilities: ");
		for (int i = 0; i < p.classProbabilities.length; i++) {
			if (i > 0) {
				System.out.print(",");
			}
			System.out.print(p.classProbabilities[i]);
		}
		System.out.println("");
	}
}
