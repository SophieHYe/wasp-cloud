package test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Random;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.linalg.DenseMatrix;
import org.apache.spark.ml.linalg.Matrices;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.linalg.distributed.BlockMatrix;
import org.apache.spark.mllib.linalg.distributed.IndexedRow;
import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;
import org.apache.spark.mllib.util.MLUtils;

import scala.Tuple3;
import scala.collection.Iterable;
import scala.collection.immutable.List;

import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.SingularValueDecomposition;
import org.apache.spark.mllib.linalg.SparseMatrix;
import org.apache.spark.mllib.linalg.Vector;

public class Run {

	public static void main(String[] args) throws IOException {
//		SparkConf sparkConf = new SparkConf().setAppName("wasp-cloud");
		SparkConf sparkConf = new SparkConf().setAppName("wasp-cloud").setMaster("local");
		JavaSparkContext jpc = new JavaSparkContext(sparkConf);
		Long start = System.currentTimeMillis();

		double[] densefile = new Run().readfile();
		DenseMatrix dm = (DenseMatrix) Matrices.dense(1138, 3411, densefile);
		//create random dense matrix	
//		Random rand = new Random(100);
//		DenseMatrix dm = (DenseMatrix) Matrices.rand(5000,5000,rand);
		
		JavaRDD<IndexedRow> rows = jpc.parallelize(Arrays.asList(dm.toArray())).map(f -> {
			long key = new Double(f[0]).longValue();
			double[] value = new double[f.length - 1];
			for (int i = 1; i < f.length; i++) {
				value[i - 1] = f[i];
			}
			return new IndexedRow(key, Vectors.dense(value));
		});

		IndexedRowMatrix indexrowmatrix = new IndexedRowMatrix(rows.rdd());

		// method 1: SVD
		SingularValueDecomposition svdcomput = indexrowmatrix.computeSVD(10, true, 0.1);
		System.out.println(svdcomput.s());
		Long end = System.currentTimeMillis();
		Long executiontime = (end - start) / 1000;
		System.out.println(executiontime);

	}

	public  double[] readfile() throws IOException {
		
		InputStream in = this.getClass().getResourceAsStream("/files/row1138.csv");
		
		System.out.println("*******Reading the dense matrix files******");
		String lines = "";
		BufferedReader br = new BufferedReader(new InputStreamReader(in));

		String st;
		while ((st = br.readLine()) != null) {
			lines += st;
		}		
		lines.replace("\n", ",");
		lines.replace("\r", ",");		
		Long l = (long) lines.length();
		
		System.out.println("strings~~~~~:" + l);
		String[] values = lines.split(",");

		System.out.println("The values~~~~~:" + values.length);
		double[] darray = new double[values.length];

		for (int i = 0; i < values.length; i++) {
			if(null!=values[i]&&""!=values[i]) {
				String[] p = values[i].split(".");
				if(p.length==2) {
			double dv = Double.parseDouble(values[i]);	
				
			darray[i] = dv;
			}
			}
		}
		return darray;
	}

}
