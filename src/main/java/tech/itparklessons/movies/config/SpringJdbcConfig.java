package tech.itparklessons.movies.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;

import javax.sql.DataSource;

//@EnableWebMvc
//@Configuration
//@ComponentScan("com.baeldung.jdbc")
public class SpringJdbcConfig {
//    @Bean
//    public DataSource mysqlDataSource() {
//        DriverManagerDataSource dataSource = new DriverManagerDataSource();
//        dataSource.setDriverClassName("com.postgresql.jdbc.Driver");
//        dataSource.setUrl("jdbc:postgresql://localhost:6849/movies");
//        dataSource.setUsername("postgres");
//        dataSource.setPassword("postgres");
//
//        return dataSource;
//    }
}