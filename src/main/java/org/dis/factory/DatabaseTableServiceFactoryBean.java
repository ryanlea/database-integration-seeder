package org.dis.factory;

import org.dis.*;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.simple.SimpleJdbcTemplate;
import org.springframework.util.Assert;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;

@SuppressWarnings("unused")
public class DatabaseTableServiceFactoryBean implements FactoryBean, InitializingBean {

    private DataSource dataSource;

    private String defaultSchema;

    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public void setDefaultSchema(String defaultSchema) {
        this.defaultSchema = defaultSchema;
    }

    public void afterPropertiesSet() throws Exception {
        Assert.state(dataSource != null, "dataSource is required to construct DatabaseTableService.");
    }

    public Object getObject() throws Exception {
        final AbstractDatabaseTableDao databaseTableDao = createDatabaseTableDao();
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        databaseTableDao.setJdbcTemplate(jdbcTemplate);
        databaseTableDao.setSimpleJdbcTemplate(new SimpleJdbcTemplate(dataSource));

        final DatabaseTableServiceImpl databaseTableService = new DatabaseTableServiceImpl();
        databaseTableService.setDatabaseTableDao(databaseTableDao);
        return databaseTableService;
    }

    public Class<?> getObjectType() {
        return DatabaseTableService.class;
    }

    public boolean isSingleton() {
        return true;
    }

    private AbstractDatabaseTableDao createDatabaseTableDao() {
        try {
            Connection connection = dataSource.getConnection();
            DatabaseMetaData metaData = connection.getMetaData();
            final String databaseProductName = metaData.getDatabaseProductName();
            if (databaseProductName.contains("HSQL")) {
                return new HSQLDatabaseTableDao(dataSource);
            } else if (databaseProductName.contains("Oracle")) {
                return new OracleDatabaseTableDao(defaultSchema);
            } else if (databaseProductName.contains("PostgreSQL")) {
                return new PostgresDatabaseTableDao(defaultSchema);
            } else {
                throw new RuntimeException("Cannot recognize Data base type: '" + databaseProductName + "'");
            }
        } catch (SQLException e) {
            throw new DatabaseTestException("Failed to get product name of database.", e);
        }
    }
}
