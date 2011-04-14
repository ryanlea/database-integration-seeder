package org.dis;

import org.dis.loader.DatabaseDataSetLoader;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.DependencyInjectionTestExecutionListener;

import javax.annotation.Resource;
import java.math.BigDecimal;

@RunWith(SpringJUnit4ClassRunner.class)
@TestExecutionListeners({
        DependencyInjectionTestExecutionListener.class,
        DatabaseTestExecutionListener.class
})
@DatabaseTest
public abstract class AbstractTest {

    @Resource(name = "databaseTableService")
    protected DatabaseTableService databaseTableService;

    protected DataSetBuilder builder = DataSetBuilder.dataSet();

    protected DatabaseDataSetLoader loader() {
        final DataSet dataSet = builder.create();
        return new DatabaseDataSetLoader(databaseTableService, dataSet.getDatabaseTables());        
    }

    protected static BigDecimal bd(int val) {
        return new BigDecimal(val);
    }

}
