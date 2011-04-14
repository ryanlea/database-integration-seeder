package org.dis;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.List;

/**
 * <p>
 * Represents a Database Table.  This is not a complete implementation of everything table like.  It contains:
 * <ul>
 * <li>Table Owner</li>
 * <li>Table Name</li>
 * <li>A list of child referential constraints (tables that have foreign keys pointing to this table)</li>
 * <li>A list of database triggers that impact this table</li>
 * <li>A list of parent referential constraints (tables that this table references via foreign keys).</li>
 * </ul>
 * </p>
 */
public class DatabaseTable {

    private List<DatabaseReferentialConstraint> childConstraints;
    private List<DatabaseReferentialConstraint> parentConstraints;
    private String tableName;
    private String owner;
    private boolean indexOrganized;
    private List<DatabaseTrigger> databaseTriggers;

    public DatabaseTable() {
        childConstraints = new ArrayList<DatabaseReferentialConstraint>();
        parentConstraints = new ArrayList<DatabaseReferentialConstraint>();
        databaseTriggers = new ArrayList<DatabaseTrigger>();
    }

    public DatabaseTable(final String tableName) {
        setTableName(tableName);
    }

    public DatabaseTable(final String owner, final String tableName) {
        setOwner(owner);
        setTableName(tableName);
    }

    public void setTableName(String tableName) {
        Assert.notNull(tableName, "tableName cannot be null.");
        this.tableName = tableName.toUpperCase();
    }

    public String getTableName() {
        return tableName;
    }

    public void setChildConstraints(List<DatabaseReferentialConstraint> childConstraints) {
        this.childConstraints = childConstraints;
    }

    public List<DatabaseReferentialConstraint> getChildConstraints() {
        return childConstraints;
    }

    public int getNumberOfChildConstraints() {
        return childConstraints == null ? 0 : childConstraints.size();
    }

    public void setOwner(final String owner) {
        Assert.notNull(owner, "owner cannot be null.");
        this.owner = owner.toUpperCase();
    }

    public void setDatabaseTriggers(List<DatabaseTrigger> databaseTriggers) {
        this.databaseTriggers = databaseTriggers;
    }

    public List<DatabaseTrigger> getDatabaseTriggers() {
        return databaseTriggers;
    }

    public String qualifiedName() {
///*
//        return new StringBuilder()
//                .append(owner).append(".").append(tableName).toString();
//*/
        return tableName;
    }

    public static DatabaseTable parse(String table) {
        final String[] split = table.split("\\.");
        Assert.state(split.length == 1 || split.length == 2, "invalid table structure name");
        DatabaseTable databaseTable = null;
        if (split.length == 1) {
            databaseTable = new DatabaseTable();
            databaseTable.setTableName(split[0]);
        } else if (split.length == 2) {
            databaseTable = new DatabaseTable(split[0], split[1]);
        }
        return databaseTable;
    }

    public String getOwner() {
        return owner;
    }

    public void addParentConstraint(DatabaseReferentialConstraint parentConstraint) {
        parentConstraints.add(parentConstraint);
    }

    public List<DatabaseReferentialConstraint> getParentConstraints() {
        return parentConstraints;
    }

    public boolean isIndexOrganized() {
        return indexOrganized;
    }

    public void setIndexOrganized(boolean indexOrganized) {
        this.indexOrganized = indexOrganized;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.MULTI_LINE_STYLE);
    }
}
