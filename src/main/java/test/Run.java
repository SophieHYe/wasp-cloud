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
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix;
import org.apache.spark.mllib.linalg.distributed.GridPartitioner;
import org.apache.spark.mllib.linalg.distributed.IndexedRow;
import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix;
import org.apache.spark.mllib.linalg.distributed.MatrixEntry;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;
import org.apache.spark.mllib.util.MLUtils;
import org.apache.spark.rdd.RDD;

import scala.Tuple3;
import scala.collection.Iterable;
import scala.collection.immutable.List;

import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.SingularValueDecomposition;
import org.apache.spark.mllib.linalg.SparseMatrix;
import org.apache.spark.mllib.linalg.Vector;

public class Run {

	public static void main(String[] args) throws IOException {
		
		//Configuration for the cluster running
		SparkConf sparkConf = new SparkConf().setAppName("wasp-cloud");
		//Configuration for the local running
		//SparkConf sparkConf = new SparkConf().setAppName("wasp-cloud").setMaster("local[2]");	
		
		JavaSparkContext jpc = new JavaSparkContext(sparkConf);
						
		double[] densefile = new Run().readfile();
		DenseMatrix dm = (DenseMatrix) Matrices.dense(4350, 4350, densefile);
		
		Long start = System.currentTimeMillis();
		
		
		JavaRDD<IndexedRow> rows = jpc.parallelize(Arrays.asList(dm.toArray()),24).map(f -> {
			long key = new Double(f[0]).longValue();
			double[] value = new double[f.length - 1];
			for (int i = 1; i < f.length; i++) {
				value[i - 1] = f[i];
			}
			return new IndexedRow(key, Vectors.dense(value));
		});
		
		
		IndexedRowMatrix indexrowmatrix = new IndexedRowMatrix(rows.rdd());
		System.out.println("indexrowmatrix.numRows():"+indexrowmatrix.numRows());
		System.out.println("indexrowmatrix.numCols():"+indexrowmatrix.numCols());	
		
		//method 1: SVD
		SingularValueDecomposition svdcomput = indexrowmatrix.computeSVD(10, true, 0.1);
		System.out.println(svdcomput.s());
		Long end = System.currentTimeMillis();
		System.out.println((end-start)/1000);
		
		//method 2: BlockMatrix multiple the transpose of itself
		BlockMatrix blockMatrix = indexrowmatrix.toBlockMatrix();
		BlockMatrix other = blockMatrix.add(blockMatrix) ;
		blockMatrix.multiply(other.transpose());	
	
	}

	public  double[] readfile() throws IOException {
		
		InputStream in = this.getClass().getResourceAsStream("/files/matrix.csv");		
		System.out.println("*******Reading the matrix files******");
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		double[] darray = new double[18922500];
		String st;
		int count = 0;
		while ((st = br.readLine()) != null) {
			String[] columns = st.split(",");
			for(int i=0;i<4350;i++) {
				double dv = Double.parseDouble(columns[i]);
				int index= count*4350+i;
				darray[index]=dv;
			}			
			count +=1;
		}		
		return darray;
	}

}
