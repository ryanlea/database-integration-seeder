package org.dis;

import org.apache.commons.lang.StringUtils;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.jdbc.core.simple.SimpleJdbcInsert;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public class PostgresDatabaseTableDao extends AbstractDatabaseTableDao {

    private static final String DATABASE_TABLE_SQL = "select tableowner as owner, tablename as table_name from pg_tables where tablename = :tableName and tableowner = :owner";

    private static final String CHILD_TABLE_REFERENCES_SQL =
    "select con.conname as constraint_name, child_tab.relname as table_name, owner.rolname as owner from pg_constraint con join pg_class parent_tab on con.confrelid = parent_tab.oid  join pg_class child_tab on con.conrelid = child_tab.oid join pg_roles owner on child_tab.relowner = owner.oid where con.contype = 'f' and parent_tab.relname = :tableName";

    private static final String TABLE_TRIGGERS_SQL =
            "select pt.tgname as trigger_name, p.rolname as table_owner from" +
            " pg_trigger pt, pg_class t, pg_roles p where t.relname = :tableName and p.rolname = :owner and pt.tgrelid = t.relowner";

    private String defaultSchema;

    public PostgresDatabaseTableDao(String defaultSchema) {
        this.defaultSchema = defaultSchema;
    }

    public DatabaseTable loadDatabaseTable(DatabaseTable table) {
        String tableName = table.getTableName().toLowerCase();
        String owner = table.getOwner() == null ? defaultSchema : table.getOwner().toLowerCase();
        final SqlParameterSource in = new MapSqlParameterSource()
                .addValue("tableName", tableName).addValue("owner", owner);
        return getSimpleJdbcTemplate().queryForObject(DATABASE_TABLE_SQL, new DatabaseTableRowMapper(), in);
    }

    public List<DatabaseReferentialConstraint> loadChildTableReferences(final DatabaseTable table) {
        String tableName = table.getTableName();
        String lowerCaseTableName = tableName.toLowerCase();
        String owner = table.getOwner() == null ? defaultSchema : table.getOwner();
        final SqlParameterSource in = new MapSqlParameterSource().addValue("tableName", lowerCaseTableName).addValue(
                "owner", owner);
        return getSimpleJdbcTemplate().query(CHILD_TABLE_REFERENCES_SQL, new DatabaseReferentialConstraintMapper(), in);
    }

    public List<DatabaseTrigger> getTriggersForTable(DatabaseTable table) {
        String tableName = table.getTableName();
        String lowerCaseTableName = tableName.toLowerCase();
        String owner = table.getOwner() == null ? defaultSchema : table.getOwner();
        final SqlParameterSource in = new MapSqlParameterSource().addValue("tableName", lowerCaseTableName).addValue(
                "owner", owner);
        return getSimpleJdbcTemplate().query(TABLE_TRIGGERS_SQL, new DatabaseTriggerRowMapper(table), in);
    }

    protected SimpleJdbcInsert createSimpleJdbcInsert(DatabaseTable table) {
        return new SimpleJdbcInsert(getJdbcTemplate())
                .withTableName(table.getTableName().toLowerCase());
    }


    private static final class DatabaseTableRowMapper implements RowMapper<DatabaseTable> {
        public DatabaseTable mapRow(ResultSet rs, int rowNum) throws SQLException {
            final DatabaseTable table = new DatabaseTable();
            table.setOwner(StringUtils.upperCase(rs.getString("owner")));
            table.setTableName(StringUtils.upperCase(rs.getString("table_name")));
            table.setIndexOrganized(false);
            return table;
        }
    }

    private static final class DatabaseReferentialConstraintMapper implements RowMapper<DatabaseReferentialConstraint> {

        public DatabaseReferentialConstraint mapRow(ResultSet rs, int rowNum) throws SQLException {
            final DatabaseTable table = new DatabaseTable();
            table.setOwner(StringUtils.upperCase(rs.getString("owner")));
            table.setTableName(StringUtils.upperCase(rs.getString("table_name")));

            final DatabaseReferentialConstraint constraint = new DatabaseReferentialConstraint();
            constraint.setConstraintName(StringUtils.upperCase(rs.getString("constraint_name")));
            constraint.setChildTable(table);

            return constraint;
        }
    }

    private static final class DatabaseTriggerRowMapper implements RowMapper<DatabaseTrigger> {

        private final DatabaseTable databaseTable;

        private DatabaseTriggerRowMapper(DatabaseTable databaseTable) {
            this.databaseTable = databaseTable;
        }

        public DatabaseTrigger mapRow(ResultSet rs, int rowNum) throws SQLException {
            final DatabaseTrigger trigger = new DatabaseTrigger();
            trigger.setDatabaseTable(databaseTable);
            trigger.setOwner(StringUtils.upperCase(rs.getString("table_owner")));
            trigger.setTriggerName(StringUtils.upperCase(rs.getString("trigger_name")));
            return trigger;
        }
    }
}
