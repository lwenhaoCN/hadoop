package cn.lwenhao.test;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class TestArray {

	
	public static void main(String[] args) {
		
		JavaSparkContext  sc = new JavaSparkContext("local","SparkTest");


		List<String> list =  new ArrayList<String>();
		list.add("11,22,33,44,55");
		list.add("aa,bb,cc,dd,ee");
		list.add("1a,2b,3c,4d,5e");
		
		JavaRDD<String> list2 = sc.parallelize(list);

		
		
		System.out.println(list2.count());
	
		
		JavaRDD<String> filterRdd = list2.filter(new Function<String, Boolean>() {
			@Override
			public Boolean call(String v1) throws Exception {
				
				if(v1.contains(",")){
					return true;
				}
				return false;
			}
		});
		
		List<String> strs = filterRdd.collect();
	
		for(String str : strs){
			System.out.println("=================:"+str);
		}
		
		//System.out.println("============"+ mapRdd.collect());
		
//		
//		// Creates a DataFrame having a single column named "line"
//		JavaRDD<String> textFile = sc.textFile("hdfs://192.168.1.162:9001//flume//20190220//02//events-hive-.1550602799497");
//		JavaRDD<Row> rowRDD = textFile.map(RowFactory::create);
//		List<StructField> fields = Arrays.asList(
//		  DataTypes.createStructField("line", DataTypes.StringType, true));
//		StructType schema = DataTypes.createStructType(fields);
//		DataFrame df = sqlContext.createDataFrame(rowRDD, schema);
//
//		DataFrame errors = df.filter(col("line").like("%ERROR%"));
//		// Counts all the errors
//		errors.count();
//		// Counts errors mentioning MySQL
//		errors.filter(col("line").like("%MySQL%")).count();
//		// Fetches the MySQL errors as an array of strings
//		errors.filter(col("line").like("%MySQL%")).collect();
	}
}
