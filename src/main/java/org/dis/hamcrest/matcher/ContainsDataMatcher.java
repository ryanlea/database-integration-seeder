package org.dis.hamcrest.matcher;

import org.dis.ColumnAndValue;
import org.dis.DataSet;
import org.dis.loader.DataSetLoader;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.springframework.util.Assert.isInstanceOf;

public class ContainsDataMatcher extends TypeSafeMatcher<DataSetLoader> {

    private final TimedPeriod timedPeriod;

    private final DataSet expectedDataSet;

    // For display
    private transient List<ColumnAndValue> unmatched = new ArrayList<ColumnAndValue>();

    private DataSetMatcher dataSetMatcher;
    public static final TimedPeriod TWO_SECONDS = new TimedPeriod(2000L);

    public ContainsDataMatcher(final DataSet expectedDataSet, final TimedPeriod timedPeriod) {
        this.expectedDataSet = expectedDataSet;
        this.timedPeriod = timedPeriod;
    }

    @Override
    public boolean matchesSafely(final DataSetLoader loader) {
        boolean contains = contains(loader);

        if (!contains) {
            timedPeriod.start();
        }

        while (!contains && timedPeriod.notExpired()) {
            timedPeriod.pause();
            contains = contains(loader);
        }
        return contains;
    }

    private boolean contains(final DataSetLoader loader) {
        final DataSet loadedDataSet = loader.load();
        final List<DataSet.DataSetRow> expectedDataSetRows = expectedDataSet.getRows();

        dataSetMatcher = new DataSetMatcher();

        for (final DataSet.DataSetRow expectedDataSetRow : expectedDataSetRows) {

            RowMatcher rowMatcher = new RowMatcher();

            final List<DataSet.DataSetRow> loadedRows = loadedDataSet.getRowsFor(expectedDataSetRow.getDatabaseTable());
            for (final DataSet.DataSetRow loadedRow : loadedRows) {
                for (final Map.Entry<String, Object> expectedRowEntry : expectedDataSetRow.getValues().entrySet()) {
                    // check each column of the expected rows to determine if the expected values match this row

                    isInstanceOf(Matcher.class, expectedRowEntry.getValue(), "Expected DataSetRow column values ");
                    final String colName = expectedRowEntry.getKey();
                    final Matcher colMatcher = (Matcher) expectedRowEntry.getValue();

                    final Object loadedColVal = loadedRow.getValueFor(colName);

                    ColumnMatcher columnMatcher = new ColumnMatcher(colMatcher, loadedColVal);
                    rowMatcher.addColumnMatcher(colName, columnMatcher);
                }

                if (rowMatcher.isMatched()) {
                    // this row is matched, clear all the failures and move onto the next row
                    dataSetMatcher.rowMatched(expectedDataSetRow, rowMatcher);
                    break;
                } else {
                    dataSetMatcher.rowMismatched(expectedDataSetRow, rowMatcher);
                }
            }


        }

        boolean contains = dataSetMatcher.isMatched();
        return contains;
    }

    public void describeTo(final Description description) {
        StringBuilder builder = new StringBuilder();
        builder.append(expectedDataSet);

        if (dataSetMatcher.hasUnmatchedRows()) {
            builder.append("\n--------------------------------------------------------------------------------");
            builder.append("\nFailed to match:\n");
            DataSet unmatchedDataSet = dataSetMatcher.getUnmatchedDataSet();
            builder.append(unmatchedDataSet);
            builder.append("--------------------------------------------------------------------------------");
        }
        
        description.appendText(builder.toString());
    }

    public static Matcher<DataSetLoader> containsData(final DataSet expectedDataSet, final TimedPeriod timedPeriod) {
        return new ContainsDataMatcher(expectedDataSet, timedPeriod);
    }

    public static Matcher<DataSetLoader> containsData(final DataSet expectedDataSet) {
        return containsData(expectedDataSet, TWO_SECONDS);
    }

    private static class DataSetMatcher {

        private final MultiValueMap<DataSet.DataSetRow, RowMatcher> rowMatchersByExpectedDataSetRow;

        private DataSetMatcher() {
            rowMatchersByExpectedDataSetRow = new LinkedMultiValueMap<DataSet.DataSetRow, RowMatcher>();
        }

        public void add(final DataSet.DataSetRow expectedDataSetRow, final RowMatcher rowMatcher) {
            rowMatchersByExpectedDataSetRow.add(expectedDataSetRow, rowMatcher);
        }

        public boolean isMatched() {
            boolean matched = !rowMatchersByExpectedDataSetRow.isEmpty();
            for (Map.Entry<DataSet.DataSetRow, List<RowMatcher>> rowMatchingStatusesEntry : rowMatchersByExpectedDataSetRow.entrySet()) {
                for (RowMatcher rowMatcher : rowMatchingStatusesEntry.getValue()) {
                    matched = matched && rowMatcher.isMatched();
                }
            }
            return matched;
        }

        public void rowMatched(DataSet.DataSetRow expectedDataSetRow, RowMatcher rowMatcher) {
            rowMatchersByExpectedDataSetRow.remove(expectedDataSetRow);
            rowMatchersByExpectedDataSetRow.add(expectedDataSetRow, rowMatcher);
        }

        public void rowMismatched(DataSet.DataSetRow expectedDataSetRow, RowMatcher rowMatcher) {
            rowMatchersByExpectedDataSetRow.add(expectedDataSetRow, rowMatcher);
        }

        public boolean hasUnmatchedRows() {
            return !isMatched();
        }

        public DataSet getUnmatchedDataSet() {
            DataSet unmatchedDataSet = new DataSet();
            for (DataSet.DataSetRow row : rowMatchersByExpectedDataSetRow.keySet()) {
                RowMatcher rowMatcher = rowMatchersByExpectedDataSetRow.getFirst(row);
                if (!rowMatcher.isMatched()) {
                    unmatchedDataSet.addRow(row);
                }
            }
            return unmatchedDataSet;
        }
    }

    private static class RowMatcher {

        private final Map<String, ColumnMatcher> columns;

        private RowMatcher() {
            this.columns = new HashMap<String, ColumnMatcher>();
        }

        public void addColumnMatcher(String colName, ColumnMatcher columnMatcher) {
            columns.put(colName, columnMatcher);
        }

        public boolean isMatched() {
            boolean matched = !columns.isEmpty();
            for (final ColumnMatcher columnMatcher : columns.values()) {
                matched = matched && columnMatcher.isMatched();
            }
            return matched;
        }
    }

    private static class ColumnMatcher {

        private final Matcher matcher;
        private final Object actual;
        private final boolean matched;

        ColumnMatcher(Matcher matcher, Object actual) {
            this.matcher = matcher;
            this.actual = actual;
            this.matched = matcher.matches(actual);
        }

        public boolean isMatched() {
            return matched;
        }
    }
}
