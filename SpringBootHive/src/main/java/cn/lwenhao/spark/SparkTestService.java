package cn.lwenhao.spark;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import scala.Tuple2;
import cn.lwenhao.common.HBaseService;

@Service
public class SparkTestService {
	private static final Pattern SPACE = Pattern.compile(" ");

    @Autowired
    private JavaSparkContext sc;
    
    @Autowired
    private HBaseService hbaseService;
    
    @Autowired
	@Qualifier("hiveDruidTemplate")
	private JdbcTemplate hiveDruidTemplate;
 
	@Autowired
	@Qualifier("hiveJdbcTemplate")
	private JdbcTemplate hiveJdbcTemplate;

    public Map<String, Object> sparkDemo() {

        Map<String, Object> result = new HashMap<String, Object>();
        //JavaRDD<String> lines = sc.textFile("C:/Users/Administrator/Desktop/general.log").cache();
        JavaRDD<String> lines = sc.textFile("hdfs://172.16.100.88:8020//lwenhao//cloudera-scm-server.log").cache();

        lines.map(
                str -> {
                    System.out.println(str);
                    return str;
                });

        System.out.println();
        System.out.println("-------------------------------------------------------");
        System.out.println(lines.count());

        JavaRDD<String> words = lines
                .flatMap(str -> Arrays.asList(SPACE.split(str)).iterator());
        


        JavaPairRDD<String, Integer> ones = words
                .mapToPair(str -> new Tuple2<String, Integer>(str, 1));

        JavaPairRDD<String, Integer> counts = ones
                .reduceByKey((Integer i1, Integer i2) -> (i1 + i2));

        JavaPairRDD<Integer, String> temp = counts
                .mapToPair(tuple -> new Tuple2<Integer, String>(tuple._2, tuple._1));

        JavaPairRDD<String, Integer> sorted = temp
                .sortByKey(false)
                .mapToPair(tuple -> new Tuple2<String, Integer>(tuple._2, tuple._1));

        System.out.println();
        System.out.println("-------------------------------------------------------");
        System.out.println(sorted.count());

        List<Tuple2<String, Integer>> output = sorted.collect();
        for (Tuple2<String, Integer> tuple : output) {
        	//System.out.println("单词："+tuple._1()+"  " + "出现次数:" + tuple._2());
            result.put("单词："+ tuple._1(),"出现次数:" + tuple._2());
        }

        return result;
    }
    
    
    public Map<String, Object> sparkLocal() {

        Map<String, Object> result = new HashMap<String, Object>();
        JavaRDD<String> lines = sc.textFile("C://Users//Administrator//Desktop//cloudera-scm-server.log").cache();
        //JavaRDD<String> lines = sc.textFile("hdfs://172.16.100.91:8020//impalaTest//cmf-server-perf.log").cache();

        lines.map(
                str -> {
                    System.out.println(str);
                    return str;
                });

        System.out.println();
        System.out.println("-------------------------------------------------------");
        System.out.println(lines.count());

        JavaRDD<String> words = lines
                .flatMap(str -> Arrays.asList(SPACE.split(str)).iterator());
        


        JavaPairRDD<String, Integer> ones = words
                .mapToPair(str -> new Tuple2<String, Integer>(str, 1));

        JavaPairRDD<String, Integer> counts = ones
                .reduceByKey((Integer i1, Integer i2) -> (i1 + i2));

        JavaPairRDD<Integer, String> temp = counts
                .mapToPair(tuple -> new Tuple2<Integer, String>(tuple._2, tuple._1));

        JavaPairRDD<String, Integer> sorted = temp
                .sortByKey(false)
                .mapToPair(tuple -> new Tuple2<String, Integer>(tuple._2, tuple._1));

        System.out.println();
        System.out.println("-------------------------------------------------------");
        System.out.println(sorted.count());

        List<Tuple2<String, Integer>> output = sorted.collect();
        for (Tuple2<String, Integer> tuple : output) {
        	//System.out.println("单词："+tuple._1()+"  " + "出现次数:" + tuple._2());
            result.put("单词："+ tuple._1(),"出现次数:" + tuple._2());
        }

        return result;
    }
    
    public Map<String, Object> sparkMysql() {
    	Map<String, Object> result = new HashMap<String, Object>();
    	
    	SparkConf conf = new SparkConf();
		conf.setAppName("TestMysql");
		conf.setMaster("local[*]");
		sc = null;
	    //SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
		sc = new JavaSparkContext(conf);
    	
        //JavaRDD<String> lines = sc.textFile("hdfs://192.168.1.237:9001//flume/MySQL_20190226/18/events-mysql-.1551177180459").cache();

        JavaRDD<String> lines = sc.textFile("C://Users//Administrator//Desktop//cloudera-scm-server.log").cache();
        
	    JavaRDD<String> filterRdd = lines.filter(new Function<String, Boolean>() {

			@Override
			public Boolean call(String v1) throws Exception {
				
				if(v1.contains("INFO")){
				//if(v1.contains("INSERT INTO his_agent_status")){
					return true;
				}
				return false;
			}
		});
//		
//		List<String> strs = filterRdd.collect();
	
//		org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
//		conf.set("hbase.zookeeper.quorum","192.168.1.237" );
//		conf.set("hbase.client.keyvalue.maxsize","500000");
//    	hbaseService = new HBaseService(conf);
		
		
//		for(String str : strs){
//			System.out.println("=================:"+str);
//			String[] temp_arr = str.split("VALUES");
//			String temp = temp_arr[1].substring(1);
//			String[] values = temp.split(",");
//            hbaseService.putData("test_base","66804_00000","f",new String[]{"project_id","varName","coefs","pvalues","tvalues","create_time"},new String[]{values[0].substring(1),values[1],values[2],values[3],values[4],values[values.length-1].substring(0,values[values.length-1].length()-1)});
//		}
//		System.out.println("============开始向hive添加数据===========");
//		StringBuffer sql = new StringBuffer("INSERT INTO TABLE  user_sample(user_num,user_name,user_gender,user_age) VALUES ");
//		sql.append("(222,'bb','M',"+strs.size()+")");
//		try {
//			hiveDruidTemplate.execute(sql.toString());
//		} catch (DataAccessException dae) {
//			dae.printStackTrace();
//		}
//		System.out.println("============开始向hive添加数据完成===========");
	    
    	return result;
    }

}
