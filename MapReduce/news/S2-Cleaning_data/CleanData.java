import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CleanData {

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: CleanData <input path> <output path>");
      System.exit(-1);
    }
    
    Job job = Job.getInstance();
    job.setJarByClass(CleanData.class);
    job.setJobName("Number Of Records per date and source");

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    job.setMapperClass(CleanDataMapper.class);
    job.setCombinerClass(CleanDataReducer.class);
    job.setReducerClass(CleanDataReducer.class);

    job.setNumReduceTasks(1);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}