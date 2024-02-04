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


public class RefiningMapper
    extends Mapper<LongWritable, Text, Text, FloatWritable> {
  

  private boolean isFloat(String str) {
    try {
        Float.parseFloat(str);
        return true;
    } catch (NumberFormatException e) {
        return false;
    }
  }
    
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    
        String[] columns = value.toString().split(",");

        // Remove the heading row
        if(columns[0]!="poll_id"){
          String result = columns[0] + "," + columns[1] + "," + columns[2] + "," + columns[3] + "," + columns[4].replace('-', '/') + "," + columns[5].replace('-', '/') + "," + columns[6]
          + "," + columns[7] + "," + columns[8] + "," + columns[9]; //standardizinf date format

          if(isFloat(columns[10])){
            context.write(new Text(result), new FloatWritable(Float.parseFloat(columns[10])));
          }
          
        }
        
  }
}