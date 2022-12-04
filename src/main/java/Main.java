import java.util.Arrays;

public class Main {

    static final int N = 100;
    public static void main(String[] args) {

        int[] records = {70, 70, 66, 60, 55};
        int[] costs =    {1, 2,  3, 4,  8};
        int[] files = new int[records.length];

        for (int i=0; i<records.length; i++){
            files[i] = (int) Math.floor(N * (1 - Math.pow(((N-1)/(N*1.0)), records[i])));
        }

        System.out.println(Arrays.toString(files));

        for (int i=0; i<records.length; i++){
            System.out.println("i=" + (i + 1) + ", cost = " + (costs[i] + toFiles(records[i])));
        }

        System.out.println("----------------------------------");

        for (int i=0; i<records.length; i++){
            for (int j=0; j<records.length; j++){
                if (i < j) {
                    System.out.println("i=" + (i + 1) + ", j=" + (j + 1) + ", cost = " + (costs[i] + costs[j] + toFiles((records[i] * records[j]) / N)));
                }
            }
        }

        System.out.println("----------------------------------");

        for (int i=0; i<records.length; i++){
            for (int j=0; j<records.length; j++){
                for (int k=0; k<records.length; k++) {
                    if (i < j && j < k) {
                            System.out.println("i=" + (i + 1) + ", j=" + (j + 1) + ", k=" + (k + 1) + ", cost = " + (costs[i] + costs[j] + costs[k]
                                    + toFiles((records[i] * records[j] * records[k]) / (N * N))));
                    }
                }
            }
        }

        System.out.println("----------------------------------");

        for (int i=0; i<records.length; i++){
            for (int j=0; j<records.length; j++){
                for (int k=0; k<records.length; k++) {
                    for (int l=0; l<records.length; l++) {
                        if (i < j && j < k && k< l) {
                            System.out.println("i=" + (i + 1) + ", j=" + (j + 1) + ", k=" + (k + 1) + ", l=" + (l + 1) + ", cost = " +
                                    (costs[i] + costs[j] + costs[k] + costs[l]
                                    + toFiles((records[i] * records[j] * records[k] * records[l]) / (N * N * N))));
                        }
                    }
                }
            }
        }

        System.out.println("----------------------------------");

        System.out.println("1,2,3,4,5" + ", cost = " +
        (costs[0] + costs[1] + costs[2] + costs[3] + costs[4]
                + toFiles((records[0] * records[1] * records[2] * records[3] * records[4]) / (N * N * N * N))));
    }

    static int toFiles(int r){
        return (int) Math.floor(N * (1 - Math.pow(((N-1)/(N*1.0)), r)));
    }
}
