package org.dis;

import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public class OracleDatabaseTableDao extends AbstractDatabaseTableDao {

    private static final String DATABASE_TABLE_SQL = "select owner, table_name, iot_type from all_tables where table_name = :tableName and owner = :owner";

    private static final String CHILD_TABLE_REFERENCES_SQL = "select r.owner, r.table_name, r.constraint_name from all_constraints t join all_constraints r on t.constraint_name = r.r_constraint_name where t.table_name = :tableName and r.owner = :owner";

    private static final String TABLE_TRIGGERS_SQL = "select trigger_name, table_owner from all_triggers where table_name = :tableName and owner = :owner";

    private String defaultSchema;

    public OracleDatabaseTableDao(String defaultSchema) {
        this.defaultSchema = defaultSchema == null ? null : defaultSchema.toUpperCase();
    }

    public DatabaseTable loadDatabaseTable(DatabaseTable table) {
        String owner = table.getOwner() == null ? defaultSchema : table.getOwner();
        final SqlParameterSource in = new MapSqlParameterSource()
                .addValue("tableName", table.getTableName()).addValue("owner", owner);
        return getSimpleJdbcTemplate().queryForObject(DATABASE_TABLE_SQL, new DatabaseTableRowMapper(), in);
    }

    public List<DatabaseReferentialConstraint> loadChildTableReferences(final DatabaseTable table) {
        String owner = table.getOwner() == null ? defaultSchema : table.getOwner();
        final SqlParameterSource in =
                new MapSqlParameterSource().addValue("tableName", table.getTableName()).addValue("owner", owner);
        return getSimpleJdbcTemplate().query(CHILD_TABLE_REFERENCES_SQL, new DatabaseReferentialConstraintMapper(), in);
    }

    public List<DatabaseTrigger> getTriggersForTable(DatabaseTable table) {
        String owner = table.getOwner() == null ? defaultSchema : table.getOwner();
        final SqlParameterSource in =
                new MapSqlParameterSource().addValue("tableName", table.getTableName()).addValue("owner", owner);
        return getSimpleJdbcTemplate().query(TABLE_TRIGGERS_SQL, new DatabaseTriggerRowMapper(table), in);
    }

    private static final class DatabaseTableRowMapper implements RowMapper<DatabaseTable> {
        public DatabaseTable mapRow(ResultSet rs, int rowNum)
                throws SQLException {
            final DatabaseTable table = new DatabaseTable();
            table.setOwner(rs.getString("owner"));
            table.setTableName(rs.getString("table_name"));
            table.setIndexOrganized("IOT".equals(rs.getString("iot_type")));
            return table;
        }
    }

    private static final class DatabaseReferentialConstraintMapper implements RowMapper<DatabaseReferentialConstraint> {

        public DatabaseReferentialConstraint mapRow(ResultSet rs, int rowNum)
                throws SQLException {
            final DatabaseTable table = new DatabaseTable();
            table.setOwner(rs.getString("owner"));
            table.setTableName(rs.getString("table_name"));

            final DatabaseReferentialConstraint constraint = new DatabaseReferentialConstraint();
            constraint.setConstraintName(rs.getString("constraint_name"));
            constraint.setChildTable(table);

            return constraint;
        }
    }

    private static final class DatabaseTriggerRowMapper implements RowMapper<DatabaseTrigger> {

        private final DatabaseTable databaseTable;

        private DatabaseTriggerRowMapper(DatabaseTable databaseTable) {
            this.databaseTable = databaseTable;
        }

        public DatabaseTrigger mapRow(ResultSet rs, int rowNum)
                throws SQLException {
            final DatabaseTrigger trigger = new DatabaseTrigger();
            trigger.setDatabaseTable(databaseTable);
            trigger.setOwner(rs.getString("table_owner"));
            trigger.setTriggerName(rs.getString("trigger_name"));
            return trigger;
        }
    }

}
