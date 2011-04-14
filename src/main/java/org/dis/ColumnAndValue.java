package org.dis;

public class ColumnAndValue {

    private final String tableName;

    private final String column;

    private final Object value;

    private final Object expectedValue;

    public ColumnAndValue(String tableName, String column, Object value, Object expectedValue) {
        this.tableName = tableName;
        this.column = column;
        this.value = value;
        this.expectedValue = expectedValue;
    }

    public String getTableName() {
        return tableName;
    }

    public String getColumn() {
        return column;
    }

    public Object getValue() {
        return value;
    }

    public Object getExpectedValue() {
        return expectedValue;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("Table=").append(tableName).append(", column=").append(column).append(", expected value='").append(
                expectedValue).append("', actual value='").append(value).append("'");
        return builder.toString();
    }
}
