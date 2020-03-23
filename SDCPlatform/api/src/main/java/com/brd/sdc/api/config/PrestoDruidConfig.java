package com.brd.sdc.api.config;

import com.alibaba.druid.pool.DruidDataSource;
import lombok.Data;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;

/**
 * @author jiangqingsong
 * @description
 * @date 2020-03-16 12:31
 */
@Configuration
@Data
public class PrestoDruidConfig {
    @Bean(name = "prestoDataSource")
    @Primary
    @ConfigurationProperties(prefix = "presto")
    public DataSource prestoDataSource() {
        return DataSourceBuilder.create().build();
    }

    @Bean(name = "prestoTemplate")
    @Primary
    public JdbcTemplate prestoJdbcTemplate(@Qualifier("prestoDataSource") DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }
}
