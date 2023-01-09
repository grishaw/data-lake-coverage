package bqcpp;

public class Clause {

    // number of files in the data lake
    final long F = 10000;

    // (F -1) / F
    final double factor = 0.9999;

    // number of index files read from the storage
    long cost;

    // estimated number of result records
    long result;

    ClauseType type;

    String columnName;

    public String columnValue1;

    public String columnValue2;

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

    public long getTotalCost(){

        long filesEstimation = (long) (F * (1 - Math.pow(factor, result)));

        return filesEstimation + cost;
    }
}
