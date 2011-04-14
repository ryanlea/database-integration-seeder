package org.dis;

import org.apache.commons.collections.map.CaseInsensitiveMap;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import java.util.*;

public class DataSet {

    private final MultiValueMap<String, DataSetRow> rows;

    public DataSet() {
        rows = new LinkedMultiValueMap<String, DataSetRow>();
    }

    public void addRow(final DataSetRow row) {
        rows.add(row.getDatabaseTable().getTableName().toUpperCase(), row);
    }

    public Collection<DatabaseTable> getDatabaseTables() {
        final Collection<DatabaseTable> tables = new HashSet<DatabaseTable>();
        for (final String tableName : rows.keySet()) {
            tables.add(rows.getFirst(tableName).getDatabaseTable());
        }
        return tables;
    }

    public List<DataSetRow> getRows() {
        final List<DataSetRow> mergedRows = new ArrayList<DataSetRow>();
        for (final List<DataSetRow> tableRows : rows.values()) {
            mergedRows.addAll(tableRows);
        }
        return mergedRows;
    }

    public List<DataSetRow> getRowsFor(final DatabaseTable table) {
        List<DataSetRow> rowsFor = rows.get(table.getTableName());
        return rowsFor == null ? new ArrayList<DataSetRow>() : rowsFor;
    }

    public void addRows(final Collection<DataSetRow> dataSetRows) {
        for (final DataSetRow row : dataSetRows) {
            addRow(row);
        }
    }

    public String toString() {
        StringBuilder builder = new StringBuilder();
        for (Map.Entry<String, List<DataSetRow>> tableNameAndRow : rows.entrySet()) {
            builder.append('\n');
            builder.append("TABLE: ").append(tableNameAndRow.getKey()).append("\n");
            List<DataSetRow> rows = tableNameAndRow.getValue();
            Set<String> colNames = getColumnNames(rows);
            for (String colName : colNames) {
                builder.append(colName).append(",");
            }
            builder.deleteCharAt(builder.length() - 1);
            builder.append("\n");
            for (DataSetRow dataSetRow : rows) {
                for (String colName : colNames) {
                    builder.append(dataSetRow.getValueFor(colName)).append(",");
                }
                builder.deleteCharAt(builder.length() - 1);
                builder.append("\n");
            }
        }
        return builder.toString();
    }

    private static Set<String> getColumnNames(List<DataSetRow> rows) {
        Set<String> colNames = new TreeSet<String>();
        for (DataSetRow row : rows) {
            colNames.addAll(row.getValues().keySet());
        }
        return colNames;
    }

    public static final class DataSetRow {

        private DatabaseTable databaseTable;

        /** Column name vs column value */
        private final CaseInsensitiveMap values;

//        private final Map<String, Object> values;

        public DataSetRow() {
            values = new CaseInsensitiveMap();
        }

        public void setDatabaseTable(DatabaseTable databaseTable) {
            this.databaseTable = databaseTable;
        }

        public void addColumnValue(String columnName, Object columnValue) {
            values.put(columnName.toUpperCase(), columnValue);
        }

        public DatabaseTable getDatabaseTable() {
            return databaseTable;
        }

        public Map<String, Object> getValues() {
            return values;
        }

        public boolean hasData() {
            return !values.isEmpty();
        }

        public String toString() {
            StringBuilder builder = new StringBuilder();
            for (Object o : values.entrySet()) {
                Map.Entry me = (Map.Entry) o;
                builder.append('\n');
                builder.append("Column=").append(me.getKey()).append(", Value='").append(me.getValue()).append("'");
            }
            return builder.toString();
        }

        public Object getValueFor(final String colName) {
            return values.get(colName);
        }
    }

}
