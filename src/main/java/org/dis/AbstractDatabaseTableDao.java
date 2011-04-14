package org.dis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.SimpleJdbcInsert;
import org.springframework.jdbc.core.simple.SimpleJdbcTemplate;
import org.springframework.jdbc.support.JdbcUtils;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public abstract class AbstractDatabaseTableDao implements DatabaseTableDao {

    private static final Logger logger = LoggerFactory.getLogger(AbstractDatabaseTableDao.class);

    private static final String DISABLE_CONSTRAINT_SQL = "alter table {0} disable constraint {1}";
    private static final String ENABLE_CONSTRAINT_SQL = "alter table {0} enable constraint {1}";
    private static final String TRUNCATE_TABLE_SQL = "truncate table {0}";
    private static final String DELETE_TABLE_SQL = "delete from {0}";
    private static final String DISABLE_TRIGGER_SQL = "alter trigger {0} disable";
    private static final String ENABLE_TRIGGER_SQL = "alter trigger {0} enable";
    private static final String CURRENT_SCN_SQL = "select current_scn from v$database";
    static final String ENABLE_ROW_MOVEMENT_SQL = "alter table {0} enable row movement";
    static final String DISABLE_ROW_MOVEMENT_SQL = "alter table {0} disable row movement";
    static final String FLASHBACK_TABLE_SQL = "flashback table {0} to scn {1}";
    private static final String SELECT_FROM_TABLE_SQL = "select * from {0}";

    private SimpleJdbcTemplate simpleJdbcTemplate;

    private JdbcTemplate jdbcTemplate;

    // This is not thread-safe
    private final Map<DatabaseTable, SimpleJdbcInsert> tableInsertions;

    AbstractDatabaseTableDao() {
        tableInsertions = new HashMap<DatabaseTable, SimpleJdbcInsert>();
    }

    public void setSimpleJdbcTemplate(SimpleJdbcTemplate simpleJdbcTemplate) {
        this.simpleJdbcTemplate = simpleJdbcTemplate;
    }

    protected SimpleJdbcTemplate getSimpleJdbcTemplate() {
        return simpleJdbcTemplate;
    }

    public void setJdbcTemplate(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public JdbcTemplate getJdbcTemplate() {
        return jdbcTemplate;
    }

    public void truncateTable(final DatabaseTable table) {
        try {
            disableConstraints(table);
            simpleJdbcTemplate.getJdbcOperations().execute(sql(TRUNCATE_TABLE_SQL, table.qualifiedName()));
        } finally {
            enableConstraints(table);
        }
    }

    public void deleteTable(final DatabaseTable table) {
        String sql = sql(DELETE_TABLE_SQL, table.qualifiedName());
        simpleJdbcTemplate.getJdbcOperations().execute(sql);
    }

    public void insertRow(DataSet.DataSetRow row) {
        final DatabaseTable table = row.getDatabaseTable();
        SimpleJdbcInsert insert;
        if (tableInsertions.containsKey(table)) {
            insert = tableInsertions.get(table);
        } else {
            insert = createSimpleJdbcInsert(table);
            tableInsertions.put(table, insert);
        }
        Map<String, Object> values = row.getValues();
        insert.execute(values);
    }

    protected SimpleJdbcInsert createSimpleJdbcInsert(DatabaseTable table) {
        return new SimpleJdbcInsert(jdbcTemplate)
                .withSchemaName(table.getOwner())
                .withTableName(table.getTableName());
    }

    public void disableTriggers(DatabaseTable table) {
        for (DatabaseTrigger trigger : table.getDatabaseTriggers()) {
            simpleJdbcTemplate.getJdbcOperations().execute(sql(DISABLE_TRIGGER_SQL, trigger.getQualifiedName()));
        }
    }

    public void enableTriggers(DatabaseTable table) {
        for (DatabaseTrigger trigger : table.getDatabaseTriggers()) {
            simpleJdbcTemplate.getJdbcOperations().execute(sql(ENABLE_TRIGGER_SQL, trigger.getQualifiedName()));
        }
    }

    public Long getCurrentScn() {
        return simpleJdbcTemplate.queryForLong(CURRENT_SCN_SQL);
    }

    /**
     * <p>
     * There are 2 types of tables that we have:
     * <ul>
     *  <li>IOT (Index Organized Tables) tables, and</li>
     *  <li>Normal tables</li>
     * </ul>
     *
     * For a normal table, the flashback sequence is:
     * <code>
     *  alter table <schema>.<table name> enable row movement;
     *  flashback table <schema>.<table name> to scn <scn #>;
     *  alter table <schema>.<table name> disable row movement;
     * </code>
     *
     * For an IOT table, the flashback sequence is:
     * <code>
     *  flashback table <schema>.<table name> to scn <scn #>.;
     * </code>
     * </p>
     * @param databaseTable     The table to flashback
     * @param databaseScn       The scn to flashback to
     */
    public void flashbackTable(DatabaseTable databaseTable, Long databaseScn) {
        if (!databaseTable.isIndexOrganized()) {
            jdbcTemplate.execute(sql(ENABLE_ROW_MOVEMENT_SQL, databaseTable.qualifiedName()));
        }
        jdbcTemplate.execute(sql(FLASHBACK_TABLE_SQL, databaseTable.qualifiedName(), databaseScn.toString()));
        if (!databaseTable.isIndexOrganized()) {
            jdbcTemplate.execute(sql(DISABLE_ROW_MOVEMENT_SQL, databaseTable.qualifiedName()));
        }
    }

    public Collection<DataSet.DataSetRow> loadDataRows(final DatabaseTable databaseTable) {
        final String sql = sql(SELECT_FROM_TABLE_SQL, databaseTable.getTableName());
        return simpleJdbcTemplate.query(sql, new DataSetRowMapper(databaseTable));
    }

    private void enableConstraints(DatabaseTable table) {
        for (DatabaseReferentialConstraint constraint : table.getChildConstraints()) {
            try {
                simpleJdbcTemplate.getJdbcOperations().execute(sql(ENABLE_CONSTRAINT_SQL, constraint.getQualifiedChildTableName(), constraint.getConstraintName()));
            } catch (Throwable t) {
                logger.error("Failed to enable constraint [" + constraint.getConstraintName() + "]", t);
            }
        }
    }

    private void disableConstraints(DatabaseTable table) {
        for (DatabaseReferentialConstraint constraint : table.getChildConstraints()) {
            simpleJdbcTemplate.getJdbcOperations().execute(sql(DISABLE_CONSTRAINT_SQL, constraint.getQualifiedChildTableName(), constraint.getConstraintName()));
        }
    }

    static String sql(final String messageFormat, final String ... args) {
        return new MessageFormat(messageFormat).format(args);
    }

    private static class DataSetRowMapper implements RowMapper<DataSet.DataSetRow> {

        private final DatabaseTable databaseTable;

        DataSetRowMapper(final DatabaseTable databaseTable) {
            this.databaseTable = databaseTable;
        }

        public DataSet.DataSetRow mapRow(final ResultSet rs, final int rowNum) throws SQLException {
            final ResultSetMetaData metaData = rs.getMetaData();
            final DataSet.DataSetRow row = new DataSet.DataSetRow();
            row.setDatabaseTable(databaseTable);
            final int columns = metaData.getColumnCount();
            for (int i = 1; i < columns + 1; i++) {
                final String columnName = JdbcUtils.lookupColumnName(metaData, i);
                row.addColumnValue(columnName, JdbcUtils.getResultSetValue(rs, i));
            }
            return row;
        }
    }
}
