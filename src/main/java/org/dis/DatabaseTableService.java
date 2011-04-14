package org.dis;

import java.util.Collection;

public interface DatabaseTableService {

    DatabaseTable loadDatabaseTableStructure(String tableName);

    void truncateDatabaseTable(DatabaseTable table);

    void deleteDatabaseTable(DatabaseTable table);

    DatabaseTable loadDatabaseTableStructure(DatabaseTable databaseTable);

    Long getDatabaseScn();

    void flashbackTables(Collection<DatabaseTable> databaseTables, Long databaseScn);

    void logFlashbackTables(Collection<DatabaseTable> databaseTables, Long databaseScn, StringBuilder builder);

    void deleteTables(Collection<DatabaseTable> databaseTables);

    void insertDataSet(DataSet dataSet);

    void disableTriggers(Collection<DatabaseTable> databaseTables);

    void enableTriggers(Collection<DatabaseTable> databaseTables);

    DataSet loadDataSetFromTables(Collection<DatabaseTable> databaseTables);
}
