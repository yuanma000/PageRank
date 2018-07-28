public class Driver {

    public static void main(String[] args) throws Exception{
        MatrixCellMultiplication mm = new MatrixCellMultiplication();
        MatrixCellSum ms = new MatrixCellSum();

        String transitionMatrix = args[0];
        String prMatrix = args[1];
        String unitState = args[2]; //output / subpr
        int count = Integer.parseInt(args[3]);// how many iteration do we want? 30? 40?
        String beta = args[4];

        for(int i = 0; i < count; i++){
            //                input:TMatrix    input:PRMatrix, output: subpr
            String[] args1 = {transitionMatrix, prMatrix+i, unitState+i, beta};
            MatrixCellMultiplication.main(args1);
            //               input:subpr   output: PRMatrix+1
            String[] args2 = {unitState+i, prMatrix+i, prMatrix+(i+1), beta};
            MatrixCellSum.main(args2);
        }

    }
}
