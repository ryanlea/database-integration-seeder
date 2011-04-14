package org.dis;

import org.dis.loader.XMLFileDataSetLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.test.context.TestContext;
import org.springframework.test.context.TestExecutionListener;

import java.lang.reflect.Method;
import java.util.*;

public class DatabaseTestExecutionListener implements TestExecutionListener {

    private static final Logger logger = LoggerFactory.getLogger(DatabaseTestExecutionListener.class);

    private final Map<Class, Long> databaseScns;

    private final Map<Class, Set<DataSet>> dataSets;

    public DatabaseTestExecutionListener() {
        databaseScns = new HashMap<Class, Long>();
        dataSets = new HashMap<Class, Set<DataSet>>();
    }

    /**
     * <p>
     * Load the current database scn before the test class method are executed.  This allows us to flashback at the
     * completion of all of the test methods, rather than after each one.  Flashing back takes a long time.
     * </p>
     *
     * @param testContext The context of the test
     * @throws Exception
     */
    public final void beforeTestClass(final TestContext testContext)
            throws Exception {
        final Class testClass = testContext.getTestClass();
        DatabaseTest databaseTest = AnnotationUtils.findAnnotation(testClass, DatabaseTest.class);
        if (databaseTest != null) {
            getDatabaseTableService(testContext);
            if (databaseTest.flashback()) {
                final DatabaseTableService databaseTableService = getDatabaseTableService(testContext);
                final Long databaseScn = databaseTableService.getDatabaseScn();
                databaseScns.put(testClass, databaseScn);
                logger.info("Current Database SCN is [{}]", databaseScn);
            } else {
                logger.info("No flashback present for class[{}]", testClass.getName());
            }
        }
    }

    public void prepareTestInstance(TestContext testContext)
            throws Exception {
        // do nothing
    }

    public final void beforeTestMethod(TestContext testContext)
            throws Exception {
        final Method testMethod = testContext.getTestMethod();
        final Class testClass = testContext.getTestClass();
        DatabaseTest databaseTest = AnnotationUtils.findAnnotation(testClass, DatabaseTest.class);
        if (testMethod.isAnnotationPresent(DatabaseTestData.class)) {
            if (databaseTest == null) {
                throw new DatabaseTestException(
                        "Cannot setup test data on a class that isn't specified as a DatabaseTest");
            }

            final DatabaseTestData databaseSetup = testMethod.getAnnotation(DatabaseTestData.class);
            final DatabaseTableService databaseTableService = getDatabaseTableService(testContext);
            final DataSet dataSet = loadDataSet(databaseTableService, databaseSetup);

            if (databaseTest.flashback()) {
                final Long databaseScn = databaseScns.get(testClass);
                final StringBuilder builder = new StringBuilder().append(
                        "The flashback commands required to revert these changes are:\n");
                databaseTableService.logFlashbackTables(dataSet.getDatabaseTables(), databaseScn, builder);
                logger.info(builder.toString());
            }
            saveDataSet(testClass, dataSet);
            insertDataSet(databaseTableService, dataSet);
        }
    }

    public void afterTestMethod(TestContext testContext)
            throws Exception {
        // do nothing
    }

    public final void afterTestClass(TestContext testContext)
            throws Exception {
        final Class testClass = testContext.getTestClass();
        DatabaseTest databaseTest = AnnotationUtils.findAnnotation(testClass, DatabaseTest.class);
        if (databaseTest != null) {
            if (databaseTest.flashback()) {
                final Long databaseScn = databaseScns.get(testClass);

                final Collection<DatabaseTable> databaseTables = new ArrayList<DatabaseTable>();
                for (Set<DataSet> classDataSets : dataSets.values()) {
                    for (DataSet dataSet : classDataSets) {
                        databaseTables.addAll(dataSet.getDatabaseTables());
                    }
                }
                logger.info(
                        "Flashback all tables that were previously deleted to scn [{}].  [{}] tables to be flashed back, this may take some time.",
                        new Object[]{
                                databaseScn,
                                databaseTables.size()
                        });
                final DatabaseTableService databaseTableService = getDatabaseTableService(testContext);
                databaseTableService.flashbackTables(databaseTables, databaseScn);
                logger.info("Flashback complete.");
            }
        }
    }

    private DataSet loadDataSet(final DatabaseTableService databaseTableService, final DatabaseTestData databaseSetup) {
        final String testData = databaseSetup.value();
        final XMLFileDataSetLoader dataSetLoader = new XMLFileDataSetLoader(testData);
        final DataSet dataSet = dataSetLoader.load();
        logger.info("Loaded dataset from file [{}]", testData);
        for (DataSet.DataSetRow row : dataSet.getRows()) {
            final DatabaseTable databaseTable = row.getDatabaseTable();
            row.setDatabaseTable(databaseTableService.loadDatabaseTableStructure(databaseTable));
            logger.debug("Loaded database structure for table [{}]", databaseTable.qualifiedName());
        }
        logger.info("Loaded dataset table structure.");
        return dataSet;
    }

    private void insertDataSet(final DatabaseTableService databaseTableService, final DataSet dataSet) {
        final Collection<DatabaseTable> databaseTables = dataSet.getDatabaseTables();
        logger.info("Deleting tables for dataset preparation.");
        databaseTableService.deleteTables(databaseTables);

        logger.info("Disabling triggers for dataset insertion;");
        databaseTableService.disableTriggers(databaseTables);

        logger.info("Inserting dataset into database.");
        databaseTableService.insertDataSet(dataSet);

        logger.info("Enabling triggers for dataset insertion;");
        databaseTableService.enableTriggers(databaseTables);
    }

    private void saveDataSet(Class testClass, DataSet dataSet) {
        Set<DataSet> classDataSets = dataSets.get(testClass);
        if (classDataSets == null) {
            classDataSets = new HashSet<DataSet>();
            dataSets.put(testClass, classDataSets);
        }
        classDataSets.add(dataSet);
    }

    private DatabaseTableService getDatabaseTableService(TestContext testContext) {
        DatabaseTest databaseTest = AnnotationUtils.findAnnotation(testContext.getTestClass(), DatabaseTest.class);
        final DatabaseTableService databaseTableService = (DatabaseTableService) testContext.getApplicationContext().getBean(
                databaseTest.databaseTableService());
        return databaseTableService;
    }

}
