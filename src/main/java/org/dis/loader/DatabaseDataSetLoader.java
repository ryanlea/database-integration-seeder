package org.dis.loader;

import org.dis.DataSet;
import org.dis.DatabaseTable;
import org.dis.DatabaseTableService;

import java.util.Collection;

public final class DatabaseDataSetLoader implements DataSetLoader {

    private final DatabaseTableService database;

    private final Collection<DatabaseTable> tables;

    private transient DataSet dataSet;

    public DatabaseDataSetLoader(final DatabaseTableService database, final Collection<DatabaseTable> tables) {
        this.database = database;
        this.tables = tables;
    }

    public DataSet load() {
        dataSet = database.loadDataSetFromTables(tables);
        return dataSet;
    }

    @Override
    public String toString() {
        return dataSet.toString();
    }
}