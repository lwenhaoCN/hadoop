package cn.lwenhao.test;

import java.util.List;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import cn.lwenhao.common.HBaseService;

public class TestMysql {
	
	//private static String path = "C://Users//Administrator//Desktop//IPTABLE.sql";
	private static String path = "hdfs://172.16.100.91:8020//lwenhao//IPTABLE.sql";
	private static HBaseService hbaseService;
	
	public static void main(String[] args) throws InterruptedException {
		
		System.out.println("----------开始启动----------------");
		
		SparkConf conf = new SparkConf();
		
		System.out.println("----------增加Spark客户端名称----------------");
		conf.setAppName("TestMysql");
		conf.setMaster("local[*]");
		//conf.setMaster("spark://172.16.100.91:7077");
		System.out.println("----------设置Spark名称----------------");
	    //SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
		JavaSparkContext jsc = new JavaSparkContext(conf);
	    //JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
		System.out.println("----------开始获取文件信息----------------");
	    //System.out.println("============"+jsc.textFile(path).cache());
	    
	    JavaRDD<String> lines = jsc.textFile(path).cache();
	    
	    System.out.println("----------根据规则，做出相应的计算----------------");
	    JavaRDD<String> filterRdd = lines.filter(new Function<String, Boolean>() {
			@Override
			public Boolean call(String v1) throws Exception {
				
				//if(v1.contains("INSERT INTO his_agent_status")){
				if(v1.contains("INSERT INTO IPTABLE")){
					return true;
				}
				return false;
			}
		});
	    
	    System.out.println("----------开始连接hbase----------------");
		org.apache.hadoop.conf.Configuration conf1 = HBaseConfiguration.create();
		conf1.set("hbase.zookeeper.quorum","172.16.100.91" );
		conf1.set("hbase.client.keyvalue.maxsize","500000");
    	hbaseService = new HBaseService(conf1);
		
    	 System.out.println("----------准备遍历----------------");
		List<String> strs = filterRdd.collect();
	
		 System.out.println("----------开始遍历----------------");
		int i = 0;
		for(String str : strs){
			System.out.println("=================:"+str);
			String[] temp_arr = str.split("VALUES");
			String temp = temp_arr[1].substring(1);
			String[] values = temp.split(",");
			//Thread.sleep(500);
	        hbaseService.putData("table1",String.valueOf(i),"info",new String[]{"project_id","varName","coefs","pvalues","tvalues","create_time"},new String[]{values[0].substring(1)+String.valueOf(i),values[1],values[2],values[3],values[4],values[values.length-1].substring(0,values[values.length-1].length()-1)});
	        i++;
		}
	    
	    System.out.println(strs.size());
	    System.out.println("----------结束----------------");
		
//		System.out.println("============开始向hive添加数据===========");
//		StringBuffer sql = new StringBuffer("INSERT INTO TABLE  user_sample(user_num,user_name,user_gender,user_age) VALUES ");
//
//		sql.append("("+strs.size()+",'bb','M',22)");
//		System.out.println("--------:"+sql.toString());
//		
//		HiveDruidConfig hiveDruidConfig = new HiveDruidConfig();
//		hiveDruidTemplate = hiveDruidConfig.hiveDruidTemplate(hiveDruidConfig.dataSource());
//		try {
//			hiveDruidTemplate.execute(sql.toString());
//		} catch (DataAccessException dae) {
//			dae.printStackTrace();
//		}
//		System.out.println("============开始向hive添加数据完成===========");
	    
	    
	}

}
