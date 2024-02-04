import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.HashSet;
import java.util.Set;

public class CountByPollingMethodReducer
    extends Reducer<Text, IntWritable, Text, IntWritable> {
  
  @Override
  public void reduce(Text  key, Iterable<IntWritable> values, Context context)
      throws IOException, InterruptedException {

        Set<Integer> uniqueStrings = new HashSet<>();
        
        for (IntWritable value : values) {
          uniqueStrings.add(value.get());
        }       
        context.write(key, new IntWritable(uniqueStrings.size()));   
  }
}