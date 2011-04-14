package org.dis.postgres;

import org.dis.*;
import org.junit.Test;
import org.springframework.test.context.ContextConfiguration;

import static org.dis.hamcrest.matcher.ContainsDataMatcher.containsData;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

@ContextConfiguration(locations = {
        "/org/dis/postgres/simple.spring.xml"
})
public class SimplePostgresTest extends AbstractTest {

    @DatabaseTestData("/org/dis/single-table.xml")
    @Test
    public void seedIntoSingleTable() {
        builder
                .rowFor("simple_table")
                    .col("id", equalTo(3)).col("description", equalTo("description"));
        final DataSet dataSet = builder.create();
        assertThat(loader(), containsData(dataSet));
    }
}
