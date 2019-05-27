package test;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.linalg.distributed.BlockMatrix;
import org.apache.spark.mllib.linalg.distributed.IndexedRow;
import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix;

public class Run {

	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setAppName("Mllib-test").setMaster("local");
		JavaSparkContext jpc = new JavaSparkContext(sparkConf);
		double[][] data = new double[4][4] ;
		data[0][0] = 0.0;
		data[0][1] = 2.0;
		data[0][2] = 3.0;
		data[0][3] = 4.0;
		
		data[1][0] = 1.0;
		data[1][1] = 3.0;
		data[1][2] = 4.0;
		data[1][3] = 5.0;
		
		data[2][0] = 2.0;
		data[2][1] = 4.0;
		data[2][2] = 5.0;
		data[2][3] = 6.0;
		
		data[3][0] = 3.0;
		data[3][1] = 5.0;
		data[3][2] = 6.0;
		data[3][3] = 7.0;
		
		JavaRDD<IndexedRow> rdd=jpc.parallelize(Arrays.asList(data)).map(f->{
			long key = new Double(f[0]).longValue();
			double[] value = new double[f.length-1];
			for(int i = 1;i<f.length;i++) {
				value[i-1] = f[i];
			}
			return new IndexedRow(key,Vectors.dense(value));
		});
		BlockMatrix block = new IndexedRowMatrix(rdd.rdd()).toBlockMatrix(2, 2);
		
		double[][] data1 = new double[3][3] ;
		data1[0][0] = 0.0;
		data1[0][1] = 100.0;
		data1[0][2] = 10.0;
		
		data1[1][0] = 1.0;
		data1[1][1] = 10.0;
		data1[1][2] = 100.0;
		
		data1[2][0] = 2.0;
		data1[2][1] = 1.0;
		data1[2][2] = 1000.0;

		JavaRDD<IndexedRow>  rdd1 = jpc.parallelize(Arrays.asList(data1)).map(f->{
			long key = new Double(f[0]).longValue();
			double[] value = new double[f.length-1];
			for(int i = 1;i<f.length;i++) {
				value[i-1] = f[i];
			}
			return new IndexedRow(key,Vectors.dense(value));
		});
		BlockMatrix block1 = new IndexedRowMatrix(rdd1.rdd()).toBlockMatrix(2, 2);
		block = block.multiply(block1);
		System.out.println(block);


	}	
}
