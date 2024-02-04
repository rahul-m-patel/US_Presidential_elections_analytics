import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RefiningReducer
    extends Reducer<Text, FloatWritable, Text, FloatWritable> {
  
  @Override
  public void reduce(Text  key, Iterable<FloatWritable> values, Context context)
      throws IOException, InterruptedException {

        int count = 0;
        float sum = 0.0f;
        
        for (FloatWritable value : values) {
            sum += value.get();
            count++;
        }
        
        if (count > 0) {
            float average = sum / count;
            context.write(key, new FloatWritable(average));
        }
      
  }
}