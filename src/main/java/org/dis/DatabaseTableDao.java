package org.dis;

import java.util.Collection;
import java.util.List;

public interface DatabaseTableDao {

    List<DatabaseReferentialConstraint> loadChildTableReferences(DatabaseTable table);

    void truncateTable(DatabaseTable orderedTable);

    void deleteTable(DatabaseTable orderedTable);

    List<DatabaseTrigger> getTriggersForTable(DatabaseTable table);

    void disableTriggers(DatabaseTable table);

    void enableTriggers(DatabaseTable table);

    void insertRow(DataSet.DataSetRow row);

    Long getCurrentScn();

    void flashbackTable(DatabaseTable databaseTable, Long databaseScn);

    DatabaseTable loadDatabaseTable(DatabaseTable table);

    Collection<DataSet.DataSetRow> loadDataRows(DatabaseTable databaseTable);
}
