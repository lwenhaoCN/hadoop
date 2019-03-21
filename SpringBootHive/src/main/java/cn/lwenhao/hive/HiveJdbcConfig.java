package cn.lwenhao.hive;


import org.apache.tomcat.jdbc.pool.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.JdbcTemplate;

@Configuration
public class HiveJdbcConfig {

	private static final Logger logger = LoggerFactory.getLogger(HiveJdbcConfig.class);

	@Autowired
	private Environment env;

	@Bean(name = "hiveJdbcDataSource")
	@Qualifier("hiveJdbcDataSource")
	public DataSource dataSource() {
		DataSource dataSource = new DataSource();
		dataSource.setUrl("jdbc:hive2://172.16.100.88:10000/hive");
		dataSource.setDriverClassName("org.apache.hive.jdbc.HiveDriver");
		dataSource.setUsername("root");
		dataSource.setPassword("root");
		logger.debug("Hive DataSource Inject Successfully...");
		return dataSource;
	}

	@Bean(name = "hiveJdbcTemplate")
	public JdbcTemplate hiveJdbcTemplate(@Qualifier("hiveJdbcDataSource") DataSource dataSource) {
		return new JdbcTemplate(dataSource);
	}

}