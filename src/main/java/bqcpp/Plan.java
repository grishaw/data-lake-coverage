package bqcpp;

import java.util.ArrayList;
import java.util.List;

public class Plan {

    //final long N = 5999989709L;

    // for local test
    final long N = 6001215L;

    // number of files in the data lake
    final long F = 10000;

    // (F -1) / F
    final double factor = 0.9999;

    List<Clause> clauses = new ArrayList<>();
    long cost;


    public long getTotalCost(){

        long result = clauses.get(0).result;
        for (int i=1; i<clauses.size(); i++){
            result = (long) (result * ((clauses.get(i).result * 1.0) / N));
        }

        long filesEstimation = (long) (F * (1 - Math.pow(factor, result)));

        return filesEstimation  + cost;
    }

    public long getExpectedTotalCost(Clause c){
        List<Clause> myClauses = new ArrayList<>(clauses);
        myClauses.add(c);

        long result = myClauses.get(0).result;
        for (int i=1; i<myClauses.size(); i++){
            result = (long) (result * ((myClauses.get(i).result * 1.0) / N));
        }

        long filesEstimation = (long) (F * (1 - Math.pow(factor, result)));

        return filesEstimation  + cost;
    }

    @Override
    public String toString() {
        return "Plan{" +
                "clauses=" + clauses.size() +
                ", cost=" + cost +
                ", coverage size =" + (getTotalCost() - cost) +
                ", total cost =" + getTotalCost() +
                '}';
    }
}
