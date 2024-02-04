import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class BasicStatsReducer
    extends Reducer<Text, Text, Text, Text> {

  @Override
  public void reduce(Text  key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {

        List<Double> valueList = new ArrayList<>();
        Text result = new Text(); 
        // Iterate through values and add them to the list
        for (Text value : values) {
            valueList.add(Double.parseDouble(value.toString()));
        }

        // Calculate minimum, maximum, and average
        double min = Collections.min(valueList);
        double max = Collections.max(valueList);
        double sum = 0.0;
        for (double value : valueList) {
            sum += value;
        }
        double average = sum / valueList.size();

        // Emit the key and the statistics as a formatted string
        result.set(String.format("Min: %.2f, Max: %.2f, Avg: %.2f", min, max, average));
        context.write(key, result);
  }
}