package org.dis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.support.DatabaseMetaDataCallback;
import org.springframework.jdbc.support.JdbcUtils;
import org.springframework.jdbc.support.MetaDataAccessException;
import org.springframework.util.Assert;

import javax.sql.DataSource;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class HSQLDatabaseTableDao extends AbstractDatabaseTableDao {

    private static final Logger log = LoggerFactory.getLogger(HSQLDatabaseTableDao.class);

    private final DataSource dataSource;

    public HSQLDatabaseTableDao(final DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public DatabaseTable loadDatabaseTable(final DatabaseTable table) {
        DatabaseTableCallBack callBack = new DatabaseTableCallBack(table);
        try {
            return (DatabaseTable) JdbcUtils.extractDatabaseMetaData(dataSource, callBack);
        } catch (MetaDataAccessException e) {
            throw new RuntimeException("Failed to load data base table", e);
        }
    }

    public List<DatabaseReferentialConstraint> loadChildTableReferences(final DatabaseTable table) {
        DatabaseReferentialConstraintCallBack callBack = new DatabaseReferentialConstraintCallBack(table);
        try {
            return (List<DatabaseReferentialConstraint>) JdbcUtils.extractDatabaseMetaData(dataSource, callBack);
        } catch (MetaDataAccessException e) {
            throw new RuntimeException("Failed to load child data base table references", e);
        }
    }

    public List<DatabaseTrigger> getTriggersForTable(DatabaseTable table) {
        return new ArrayList<DatabaseTrigger>();
    }

    private static final class DatabaseReferentialConstraintCallBack implements DatabaseMetaDataCallback {

        private final DatabaseTable table;

        DatabaseReferentialConstraintCallBack(final DatabaseTable table) {
            this.table = table;
        }

        public List<DatabaseReferentialConstraint> processMetaData(DatabaseMetaData dbmd) throws SQLException {
            ResultSet exportedKeys = dbmd.getExportedKeys(null, null, table.getTableName());
            final List<DatabaseReferentialConstraint> constraints = new ArrayList<DatabaseReferentialConstraint>();
            while (exportedKeys.next()) {
                String owner = exportedKeys.getString("FKTABLE_CAT");
                String childTableName = exportedKeys.getString("FKTABLE_NAME");
                String constraintName = exportedKeys.getString("FK_NAME");
                final DatabaseTable childTable = new DatabaseTable();
                childTable.setOwner(owner);
                childTable.setTableName(childTableName);
                final DatabaseReferentialConstraint constraint = new DatabaseReferentialConstraint();
                constraint.setConstraintName(constraintName);
                constraint.setChildTable(childTable);
                constraints.add(constraint);
            }
            return constraints;
        }

    }

    private static final class DatabaseTableCallBack implements DatabaseMetaDataCallback {

        private final DatabaseTable table;

        DatabaseTableCallBack(final DatabaseTable table) {
            this.table = table;
        }

        public Object processMetaData(final DatabaseMetaData dbmd) throws SQLException {
            ResultSet tables = dbmd.getTables(null, null, table.getTableName(), null);
            boolean hasNext = tables.next();
            if (hasNext) {
                String owner = tables.getString("TABLE_CAT");
                table.setOwner(owner);
                String tableName = tables.getString("TABLE_NAME");
                table.setTableName(tableName);
            }
            Assert.isTrue(!tables.next());

            return table;
        }
    }
}

