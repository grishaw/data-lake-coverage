package bqcpp;

public class Clause {

    // number of index files read from the storage
    Long cost;

    // estimated number of result records
    Long result;

    ClauseType type;

    String columnName;

    String columnValue1;

    String columnValue2;

    public Clause(ClauseType type, String columnName, String columnValue1, String columnValue2) {
        this.type = type;
        this.columnName = columnName;
        this.columnValue1 = columnValue1;
        this.columnValue2 = columnValue2;
    }

    @Override
    public String toString() {
        return "Clause{" +
                "cost=" + cost +
                ", result=" + result +
                ", type=" + type +
                ", columnName='" + columnName + '\'' +
                ", columnValue1='" + columnValue1 + '\'' +
                ", columnValue2='" + columnValue2 + '\'' +
                '}';
    }
}
