package bqcpp;

import static bqcpp.BqcppSolver.F;
import static bqcpp.BqcppSolver.FACTOR;

public class Clause {

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

    public long getTotalCost(){

        long filesEstimation = (long) (F * (1 - Math.pow(FACTOR, result)));

        return filesEstimation + cost;
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
