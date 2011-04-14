package org.dis;

import org.springframework.util.Assert;

public final class DataSetBuilder {

    private final DataSet dataSet;

    private DataSet.DataSetRow row;

    private DataSetBuilder() {
        dataSet = new DataSet();
    }

    public static DataSetBuilder dataSet() {
        return new DataSetBuilder();
    }

    /**
     * Create a new row for the specified table
     *
     * @param tableName
     * @return
     */
    public DataSetBuilder newRowForTable(final String tableName) {
        completePreviousRow();
        row = new DataSet.DataSetRow();
        row.setDatabaseTable(new DatabaseTable(tableName));
        return this;
    }

    public DataSetBuilder rowFor(final String tableName) {
        return newRowForTable(tableName);
    }

    public DataSetBuilder col(final String name, final org.hamcrest.Matcher<?> value) {
        Assert.state(row != null, "row not set, it is required when adding column values.");
        Assert.hasText(name, "Column name must not be empty");

        row.addColumnValue(name, value);
        return this;
    }

    public DataSet create() {
        completePreviousRow();
        return dataSet;
    }

    private void completePreviousRow() {
        if (row != null) {
            dataSet.addRow(row);
            row = null;
        }
    }

}
