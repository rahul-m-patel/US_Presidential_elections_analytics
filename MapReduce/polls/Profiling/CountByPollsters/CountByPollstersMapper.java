import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.lang.NumberFormatException;


public class CountByPollstersMapper
    extends Mapper<LongWritable, Text, Text, IntWritable> {
    
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    
        String[] columns = value.toString().split(",");
        String result = columns[2]; //get pollster_name
        context.write(new Text(result), new IntWritable(Integer.parseInt(columns[0]))); 
  }
}