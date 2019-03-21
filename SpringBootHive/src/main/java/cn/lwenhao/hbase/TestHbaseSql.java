package cn.lwenhao.hbase;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import cn.lwenhao.common.HBaseService;


@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = TestHbaseSql.class)
@SpringBootApplication
public class TestHbaseSql {

    //@Autowired(required=true)
    private HBaseService hbaseService;

    /**
     * 测试删除、创建表
     * @throws InterruptedException 
     */
    @Test
    public void testCreateTable() throws InterruptedException {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum","172.16.100.91" );
		conf.set("hbase.client.keyvalue.maxsize","500000");
    	hbaseService = new HBaseService(conf);
    	
        //删除表
//        hbaseService.deleteTable("t_name");
//        Thread.sleep(1000);
//    	System.out.println("=========================================");
//    	
//        //创建表
//    	System.out.println("创建中..."+sdf.format(new Date()));
//    	hbaseService.createTableBySplitKeys("mikeal-hbase-table", Arrays.asList("familyclom1","familyclom2"),hbaseService.getSplitKeys(null));
//    	System.out.println("创建成功！"+sdf.format(new Date()));
//    	
//    	Thread.sleep(1000);
//    	System.out.println("=========================================");
//    	
    	//插入三条数据
//    	System.out.println("插入数据..."+sdf.format(new Date()));
//    	for(int i=0;i<1;i++){
//            hbaseService.putData("test_base","66804_00000"+String.valueOf(i),"f",new String[]{"project_id","varName","coefs","pvalues","tvalues","create_time"},new String[]{"40866","mob_"+String.valueOf(i),"0.9416","0.0000","12.2293","null"});
//    	}
//        System.out.println("插入数据完成！"+sdf.format(new Date()));
//        //hbaseService.putData("test_base","66804_000002","f",new String[]{"project_id","varName","coefs","pvalues","tvalues","create_time"},new String[]{"40866","idno_prov","0.9317","0.0000","9.8679","null"});
//        //hbaseService.putData("test_base","66804_000003","f",new String[]{"project_id","varName","coefs","pvalues","tvalues","create_time"},new String[]{"40866","education","0.8984","0.0000","25.5649","null"});
//        Thread.sleep(1000);
//        System.out.println("=========================================");
//        
        //根据rowKey查询
//        System.out.println("根据rowKey查询数据..."+sdf.format(new Date()));
//        Map<String,String> result1 = hbaseService.getRowData("t_name","66804_000001");
//        result1.forEach((k,value) -> {
//        	System.out.println(k + "---" + value);
//        });
//        System.out.println("根据rowKey查询数据完成！"+sdf.format(new Date()));
//        Thread.sleep(1000);
//        System.out.println("=========================================");
        
        //遍历查询
        System.out.println("遍历查询数据..."+sdf.format(new Date()));
        Map<String,Map<String,String>> result2 = hbaseService.getResultScanner("test_base");
        result2.forEach((k,value) -> {
        	System.out.println(k + "---" + value);
        });
        System.out.println(result2.size()+"遍历查询数据完成！"+sdf.format(new Date()));
//        
//        //精确查询某个单元格的数据
//        String str1 = hbaseService.getColumnValue("test_base","66804_000002","f","varName");
//        System.out.println("+++++++++++精确查询某个单元格的数据+++++++++++");
//        System.out.println(str1);
//        System.out.println();
//
//        //2. 遍历查询
//        Map<String,Map<String,String>> result2 = hbaseService.getResultScanner("test_base");
//        System.out.println("+++++++++++遍历查询+++++++++++");
//        result2.forEach((k,value) -> {
//            System.out.println(k + "---" + value);
//        });
    }

}