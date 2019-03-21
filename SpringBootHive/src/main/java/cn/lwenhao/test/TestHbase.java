package cn.lwenhao.test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import cn.lwenhao.common.HBaseService;

public class TestHbase {

	
	private static org.apache.hadoop.conf.Configuration conf = null;
	
	private static HBaseService hbaseService;

 
	static {
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum","172.16.100.91" );
		conf.set("hbase.client.keyvalue.maxsize","500000");
    	hbaseService = new HBaseService(conf);
	}
	 
	
	
	public static void main(String[] args) {
		select();
	}
	
	
	@SuppressWarnings("resource")
	public static void select(){
		
		List<String> list = new ArrayList<String>();
		
		Map<String,Map<String,String>> result2 = hbaseService.getResultScanner("table1");
		result2.forEach((k,value) -> {
        	System.out.println(k + "---" + value);
        	list.add(value.toString());
        });
		
		JavaSparkContext  sc = new JavaSparkContext("local","SparkTest");
		JavaRDD<String> list2 = sc.parallelize(list);
		
		
		
		JavaRDD<String> filterRdd = list2.filter(new Function<String, Boolean>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(String v1) throws Exception {
				
				if(v1.contains(",")){
					return true;
				}
				return false;
			}
		});
		
		List<String> strs = filterRdd.collect();
		int i = 0;
		for(String str : strs){
			i++;
			System.out.println("=================:"+str);
          hbaseService.putData("test_base","1"+String.valueOf(i),"info",new String[]{"value"},new String[]{str});
		}

		System.out.println("插入新表完成,共"+i+"条数据");
		
		
	}
	
	
}
