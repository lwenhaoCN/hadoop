package cn.lwenhao.hive;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
 
/**
 * 使用 JdbcTemplate 操作 Hive
 */
@RestController
@RequestMapping("/hive2")
public class HiveJdbcTemplateController {
 
	private static final Logger logger = LoggerFactory.getLogger(HiveJdbcTemplateController.class);
 
	@Autowired
	@Qualifier("hiveDruidTemplate")
	private JdbcTemplate hiveDruidTemplate;
 
	@Autowired
	@Qualifier("hiveJdbcTemplate")
	private JdbcTemplate hiveJdbcTemplate;
 
	/**
	 * 示例：创建新表
	 */
	@RequestMapping("/table/create")
	public String createTable() {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		System.out.println("创建中..."+sdf.format(new Date()));
		
		StringBuffer sql = new StringBuffer("CREATE TABLE IF NOT EXISTS ");
		sql.append("user_sample");
		sql.append("(user_num BIGINT, user_name STRING, user_gender STRING, user_age INT)");
		sql.append("ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' "); // 定义分隔符
		sql.append("STORED AS TEXTFILE"); // 作为文本存储
 
		logger.info("Running: " + sql);
		String result = "Create table successfully...";
		try {
			// hiveJdbcTemplate.execute(sql.toString());
			hiveDruidTemplate.execute(sql.toString());
			System.out.println("创建成功！"+sdf.format(new Date()));
		} catch (DataAccessException dae) {
			result = "Create table encounter an error: " + dae.getMessage();
			logger.error(result);
		}
		return result;
 
	}
 
	/**
	 * 示例：将Hive服务器本地文档中的数据加载到Hive表中
	 */
	@RequestMapping("/table/load")
	public String loadIntoTable() {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		System.out.println("load中..."+sdf.format(new Date()));
		
		String filepath = "/home/user_sample.txt";
		String sql = "load data local inpath '" + filepath + "' into table user_sample";
		String result = "Load data into table successfully...";
		try {
			// hiveJdbcTemplate.execute(sql);
			hiveDruidTemplate.execute(sql);
			System.out.println("load成功！"+sdf.format(new Date()));
		} catch (DataAccessException dae) {
			result = "Load data into table encounter an error: " + dae.getMessage();
			logger.error(result);
		}
		return result;
	}
 
	/**
	 * 示例：向Hive表中添加数据
	 */
	@RequestMapping("/table/insert")
	public String insertIntoTable() {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		System.out.println("插入中..."+sdf.format(new Date()));
		
		String result = "Insert into table successfully...";
		
		
		StringBuffer sql = new StringBuffer("INSERT INTO TABLE  user_sample(user_num,user_name,user_gender,user_age) VALUES ");
		
		for(int i=0;i<10000;i++){
			sql.append("(222,'bb','M',22),");
		}
		sql.append("(222,'bb','M',22)");
		
		
		
		
		
		
//		String sql = "INSERT INTO TABLE  user_sample(user_num,user_name,user_gender,user_age) VALUES(222,'bb','M',22)";

		
		
		try {
			// hiveJdbcTemplate.execute(sql);
			
			hiveDruidTemplate.execute(sql.toString());
			
			System.out.println("插入成功！"+sdf.format(new Date()));
		} catch (DataAccessException dae) {
			result = "Insert into table encounter an error: " + dae.getMessage();
			logger.error(result);
		}
		return result;
	}
 
	/**
	 * 示例：删除表
	 */
	@RequestMapping("/table/delete")
	public String delete() {
		String sql = "DROP TABLE IF EXISTS user_sample";
		String result = "Drop table successfully...";
		logger.info("Running: " + sql);
		try {
			// hiveJdbcTemplate.execute(sql);
			hiveDruidTemplate.execute(sql);
		} catch (DataAccessException dae) {
			result = "Drop table encounter an error: " + dae.getMessage();
			logger.error(result);
		}
		return result;
	}

}