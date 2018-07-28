import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

//import org.apache.hadoop.mapred.lib.MultipleInputs;


public class MatrixCellMultiplication {

    public static class TransitionMatrix extends Mapper<Object, Text, Text, Text> {
        //Relations.txt   1   2,7,8,29
        //input format: fromPage\t toPage1, toPage2, toPage3, ....
        //output Key: fromPageId
        //outputValue: toPage = probability # 1/total length, because we don't know actual probability
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            String[] fromTos = value.toString().trim().split("\t");
            //if dead end then we return (only have from no end)
            if(fromTos.length < 2){
                return;
            }
            String  outputKey = fromTos[0];
            String[] tos = fromTos[1].split(",");
            for(String to: tos){
                context.write(new Text(outputKey), new Text(to+"="+(double)1/tos.length));
            }
        }
    }

    public static class PageRankMatrix extends  Mapper<Object, Text, Text, Text>{

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            //PR.txt     1   1
            //input format: page# \t PR_probability
            //output Key: pageId
            //output value: probability
            String[] pageprob = value.toString().trim().split("\t");
            context.write(new Text(pageprob[0]), new Text(pageprob[1]));
        }
    }

    public static class Multiplication extends Reducer<Text, Text, Text, Text> {

        float beta;

        public void setup(Context context){
            Configuration conf = context.getConfiguration();
            beta = conf.getFloat("beta",0.2f);
        }

        public void reduce(Text key, Iterable<Text> values, Context context) throws  IOException, InterruptedException{
            //input key: pageToId
            //input value: toPage1 = probability1, toPage2 = probability2, .... , PageRank-probability
            //outputKey: pageToId
            //outputValue: probability1 * PageRank-probability
            double page_rangk_probability = 0;
            List<String> transactionCell = new ArrayList<String>();

            for(Text value: values){
                if(value.toString().contains("=")){
                    transactionCell.add(value.toString());
                }else{
                    page_rangk_probability = Double.parseDouble(value.toString());
                }
            }

            for(String cell: transactionCell){
                String toPageId = cell.split("=")[0];
                double probability = Double.parseDouble(cell.split("=")[1]);

                //transition matrix * pageRank matrix * (1 - beta)
                double subPr = probability*page_rangk_probability*(1-beta);
                context.write(new Text(toPageId), new Text(String.valueOf(subPr)));
            }
        }

    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        //new
        conf.setFloat("beta",Float.parseFloat(args[3]));

        Job job = Job.getInstance(conf);
        job.setJarByClass(MatrixCellMultiplication.class);

        //chainMapper
        ChainMapper.addMapper(job, TransitionMatrix.class, Object.class, Text.class, Text.class, Text.class, conf);
        ChainMapper.addMapper(job, PageRankMatrix.class, Object.class, Text.class, Text.class,Text.class, conf);

        job.setReducerClass(Multiplication.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class,TransitionMatrix.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class,PageRankMatrix.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.waitForCompletion(true);
    }

}
