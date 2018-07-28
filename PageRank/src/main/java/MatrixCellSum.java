import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.fs.Path;
import java.io.IOException;
import java.text.DecimalFormat;

public class MatrixCellSum {

    public static class PassMapper extends Mapper<Object, Text, Text, DoubleWritable>{

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            //Input Value: pageToId
            //Input Value: probability1 * PageRank-probability
            //Output Key: pageId value: probability1 * PageRank-probability
            String[] idsubprob = value.toString().trim().split("\t");
            double subRank = Double.parseDouble(idsubprob[1]);

            context.write(new Text(idsubprob[0]), new DoubleWritable(subRank));
        }
    }

    //add a new mapper to read pageRanki.txt, which will add beta * pageRank matrix to result sum
    public static class BetaMapper extends Mapper<Object, Text, Text, DoubleWritable>{
        //input format: page# \t probability
        //output key: pageId
        //output value: PR_probability*beta
        float beta;

        public void setup(Context context){
            Configuration conf = context.getConfiguration();
            beta = conf.getFloat("beta",0.2f);
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            String[] pageprob = value.toString().trim().split("\t");
            double betaRank = Double.parseDouble(pageprob[1])*beta;
            context.write(new Text(pageprob[0]), new DoubleWritable(betaRank));
        }
    }

    public static class SumReducer extends Reducer<Text, DoubleWritable, Text,DoubleWritable>{

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            //Input Key: pageId value: probability1 * PageRank-probability
            double sum = 0;
            for(DoubleWritable value: values){
                sum += value.get();
            }
            DecimalFormat df = new DecimalFormat("#.0000");
            sum = Double.valueOf(df.format(sum));
            context.write(key, new DoubleWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.setFloat("beta",Float.parseFloat(args[3]));
        Job job = Job.getInstance(conf);
        job.setJarByClass(MatrixCellSum.class);

//        job.setMapperClass(PassMapper.class);
        //chainMapper
        ChainMapper.addMapper(job, PassMapper.class, Object.class, Text.class, Text.class, DoubleWritable.class, conf);
        //modified
        ChainMapper.addMapper(job, BetaMapper.class, Text.class, DoubleWritable.class, Text.class,DoubleWritable.class, conf);



        job.setReducerClass(SumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

//        FileInputFormat.addInputPath(job, new Path(args[0]));
        //set PassMapper's input path and BetaMapper's input path
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class,PassMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class,BetaMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.waitForCompletion(true);
    }
}
