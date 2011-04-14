package org.dis;

/**
 * <p>
 * Represents a Database Referential Constraint.  The constraint comprises of three elements:
 * <ul>
 *  <li>The name of the referential constraint, as viewed by the table owning the constraint</li>
 *  <li>A reference to the table that owns the constraint</li>
 *  <li>A reference to the table that is referenced by the constraint.</li>
 * </ul>
 * </p>
 */
public class DatabaseReferentialConstraint {

    private String constraintName;

    // The table that owns the constraint
    private DatabaseTable childTable;

    // The table that is referenced by the constraint
    private DatabaseTable parentTable;

    public void setConstraintName(String constraintName) {
        this.constraintName = constraintName;
    }

    public void setChildTable(DatabaseTable table) {
        this.childTable = table;
    }

    public String getQualifiedChildTableName() {
        return childTable.qualifiedName();
    }

    public DatabaseTable getChildTable() {
        return childTable;
    }

    public String getConstraintName() {
        return constraintName;
    }

    public void setParentTable(DatabaseTable parentTable) {
        this.parentTable = parentTable;
    }

    public DatabaseTable getParentTable() {
        return parentTable;
    }
}
