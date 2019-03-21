package cn.lwenhao.hive;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
 
import com.alibaba.druid.pool.DruidDataSource;
 
@Configuration
@ConfigurationProperties(prefix = "hive")
public class HiveDruidConfig {
 
	private String url;
	private String user;
	private String password;
	private String driverClassName;
	private int initialSize;
	private int minIdle;
	private int maxActive;
	private int maxWait;
	private int timeBetweenEvictionRunsMillis;
	private int minEvictableIdleTimeMillis;
	private String validationQuery;
	private boolean testWhileIdle;
	private boolean testOnBorrow;
	private boolean testOnReturn;
	private boolean poolPreparedStatements;
	private int maxPoolPreparedStatementPerConnectionSize;
 
	@Bean(name = "hiveDruidDataSource")
	@Qualifier("hiveDruidDataSource")
	public DataSource dataSource() {
		DruidDataSource datasource = new DruidDataSource();
		datasource.setUrl("jdbc:hive2://172.16.100.88:10000/hive");
		datasource.setUsername("root");
		datasource.setPassword("root");
		datasource.setDriverClassName("org.apache.hive.jdbc.HiveDriver");
 
		// pool configuration
		datasource.setInitialSize(1);
		datasource.setMinIdle(3);
		datasource.setMaxActive(50);
		datasource.setMaxWait(600000);
		datasource.setTimeBetweenEvictionRunsMillis(600000);
		datasource.setMinEvictableIdleTimeMillis(300000);
		datasource.setValidationQuery("select 1");
		datasource.setTestWhileIdle(true);
		datasource.setTestOnBorrow(false);
		datasource.setTestOnReturn(false);
		datasource.setPoolPreparedStatements(true);
		datasource.setMaxPoolPreparedStatementPerConnectionSize(20);
		return datasource;
	}
	
	// 此处省略各个属性的get和set方法
 
	@Bean(name = "hiveDruidTemplate")
	public JdbcTemplate hiveDruidTemplate(@Qualifier("hiveDruidDataSource") DataSource dataSource) {
		return new JdbcTemplate(dataSource);
	}
 
}