package org.dis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.EmptyResultDataAccessException;

import java.util.*;

import static org.dis.AbstractDatabaseTableDao.sql;
import static org.springframework.util.StringUtils.hasText;


public class DatabaseTableServiceImpl implements DatabaseTableService {

    private static final Logger logger = LoggerFactory.getLogger(DatabaseTableServiceImpl.class);

    private DatabaseTableDao databaseTableDao;

    // this is not thread-safe
    private final Map<String, DatabaseTable> cache;

    public DatabaseTableServiceImpl() {
        cache = new HashMap<String, DatabaseTable>();
    }

    public void setDatabaseTableDao(DatabaseTableDao databaseTableDao) {
        this.databaseTableDao = databaseTableDao;
    }

    /**
     * <p>
     * Loads the database structure including child foreign key reference tables
     * </p>
     * @param tableName the name of the table to load
     * @return A representation of the Database Table structure
     */
    public DatabaseTable loadDatabaseTableStructure(String tableName) {
        final DatabaseTable table = new DatabaseTable();
        table.setTableName(tableName);
        return loadDatabaseTableStructure(table);
    }

    public DatabaseTable loadDatabaseTableStructure(final DatabaseTable databaseTable) {
        final String tableName = databaseTable.getTableName();
        DatabaseTable result;
        if (cache.containsKey(tableName)) {
            result = cache.get(tableName);
        } else {
            result = loadCompleteDatabaseTableStructure(databaseTable);
            cache.put(tableName, result);
        }
        return result;
    }

    public void truncateDatabaseTable(final DatabaseTable table) {
        for (DatabaseReferentialConstraint childConstraint : table.getChildConstraints()) {
            truncateDatabaseTable(childConstraint.getChildTable());
        }
        databaseTableDao.truncateTable(table);
    }

    public void deleteDatabaseTable(DatabaseTable table) {
        for (DatabaseReferentialConstraint childConstraint : table.getChildConstraints()) {
            deleteDatabaseTable(childConstraint.getChildTable());
        }

        databaseTableDao.deleteTable(table);
    }

    public Long getDatabaseScn() {
        return databaseTableDao.getCurrentScn();
    }

    public void flashbackTables(Collection<DatabaseTable> databaseTables, Long databaseScn) {
        final Set<DatabaseTable> flashbackedTables = new HashSet<DatabaseTable>();
        for (DatabaseTable databaseTable : databaseTables) {
            flashbackTable(databaseTable, databaseScn, flashbackedTables, new FlashbackTableCallback() {
                public void flashbackTable(DatabaseTable databaseTable, Long databaseScn) {
                    databaseTableDao.flashbackTable(databaseTable, databaseScn);
                }
            });
        }
    }

    public void logFlashbackTables(Collection<DatabaseTable> databaseTables, Long databaseScn, final StringBuilder builder) {
        final Set<DatabaseTable> flashbackedTables = new HashSet<DatabaseTable>();
        for (DatabaseTable databaseTable : databaseTables) {
            flashbackTable(databaseTable, databaseScn, flashbackedTables, new FlashbackTableCallback() {
                public void flashbackTable(DatabaseTable databaseTable, Long databaseScn) {
                    if (!databaseTable.isIndexOrganized()) {
                        builder.append(sql(AbstractDatabaseTableDao.ENABLE_ROW_MOVEMENT_SQL, databaseTable.qualifiedName())).append(";\n");
                    }
                    builder.append(sql(AbstractDatabaseTableDao.FLASHBACK_TABLE_SQL, databaseTable.qualifiedName(), databaseScn.toString())).append(";\n");
                    if (!databaseTable.isIndexOrganized()) {
                        builder.append(sql(AbstractDatabaseTableDao.DISABLE_ROW_MOVEMENT_SQL, databaseTable.qualifiedName())).append(";\n");
                    }
                    builder.append("\n");
                }
            });
        }
    }

    private void flashbackTable(DatabaseTable databaseTable, Long databaseScn, final Set<DatabaseTable> flashbackedTables, final FlashbackTableCallback callback) {
        for (DatabaseReferentialConstraint parentConstraint : databaseTable.getParentConstraints()) {
            final DatabaseTable parentTable = parentConstraint.getParentTable();
            if (!flashbackedTables.contains(parentTable)) {
                flashbackTable(parentTable, databaseScn, flashbackedTables, callback);
            }
        }

        if (!flashbackedTables.contains(databaseTable)) {
            // due to the nature in which we're traversing parents and children, it is plausible that a table may
            // be flashed back via a parent, then child relationship before reaching here on its original pass.
            callback.flashbackTable(databaseTable, databaseScn);
            flashbackedTables.add(databaseTable);
        }

        for (DatabaseReferentialConstraint childConstraint : databaseTable.getChildConstraints()) {
            final DatabaseTable childTable = childConstraint.getChildTable();
            if (!flashbackedTables.contains(childTable)) {
                flashbackTable(childTable, databaseScn, flashbackedTables, callback);
            }
        }
    }

    public void disableTriggers(Collection<DatabaseTable> databaseTables) {
        for (DatabaseTable table : databaseTables) {
            databaseTableDao.disableTriggers(table);
        }
    }

    public void enableTriggers(Collection<DatabaseTable> databaseTables) {
        for (DatabaseTable table : databaseTables) {
            databaseTableDao.enableTriggers(table);
        }
    }

    public void insertDataSet(DataSet dataSet) {
        for (DataSet.DataSetRow row : dataSet.getRows()) {
            if (row.hasData()) {
                databaseTableDao.insertRow(row);
            }
        }
    }

    public void deleteTables(Collection<DatabaseTable> databaseTables) {
        for (DatabaseTable table : databaseTables) {
            deleteDatabaseTable(table);
        }
    }

    public DataSet loadDataSetFromTables(final Collection<DatabaseTable> databaseTables) {
        final DataSet dataSet = new DataSet();
        for (final DatabaseTable databaseTable : databaseTables) {
            dataSet.addRows(databaseTableDao.loadDataRows(databaseTable));            
        }
        return dataSet;
    }

    /**
     * <p>
     * This method will recursively load the table structure of the given table.  It gives primary concern to:
     * <ul>
     *  <li>Child Referential Constraints</li>
     * </ul>
     * </p>
     *
     * @param table the table we wish to load
     * @return the given table with a complete structure
     */
    private DatabaseTable loadCompleteDatabaseTableStructure(DatabaseTable table) {
        try {
            table = databaseTableDao.loadDatabaseTable(table);
        } catch (EmptyResultDataAccessException erdae) {
            StringBuilder exceptionBuilder = new StringBuilder()
                    .append("Failed to load table [").append(table.getTableName()).append("]");
            if (hasText(table.getOwner())) {
                exceptionBuilder.append(" in schema[").append(table.getOwner()).append("]");
            }
            exceptionBuilder.append(" from the database.");
            throw new DatabaseTestException(exceptionBuilder.toString(), erdae);
        }
        // load the child constraints
        final List<DatabaseReferentialConstraint> childConstraints = databaseTableDao.loadChildTableReferences(table);
        for (DatabaseReferentialConstraint childConstraint : childConstraints) {
            final DatabaseTable childTable = loadDatabaseTableStructure(childConstraint.getChildTable());
            childConstraint.setParentTable(table);
            childConstraint.setChildTable(childTable);

            childTable.addParentConstraint(childConstraint);
        }

        table.setChildConstraints(childConstraints);
        table.setDatabaseTriggers(databaseTableDao.getTriggersForTable(table));
        return table;
    }

    private interface FlashbackTableCallback {
        void flashbackTable(DatabaseTable databaseTable, Long databaseScn);
    }

}
