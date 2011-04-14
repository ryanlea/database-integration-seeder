package org.dis;

/**
 * <p>
 * Representation of a database trigger.
 * </p>
 */
public class DatabaseTrigger {

    private DatabaseTable databaseTable;

    private String triggerName;

    private String owner;

    public void setDatabaseTable(DatabaseTable databaseTable) {
        this.databaseTable = databaseTable;
    }

    public void setTriggerName(String triggerName) {
        this.triggerName = triggerName;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public String getQualifiedName() {
        return new StringBuilder()
                .append(owner).append(".").append(triggerName).toString();
    }
}
