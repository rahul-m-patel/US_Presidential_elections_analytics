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


public class BasicStatsMapper
    extends Mapper<LongWritable, Text, Text, Text> {
    
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    
        String[] columns = value.toString().split(",");
        String party_name = columns[7]; //get party_name
        String last_token = columns[9];
        String[] parts = last_token.split("\\s+");
        String score = parts[parts.length - 1];

        // Extract the remaining string without trailing spaces
        StringBuilder remainingStringBuilder = new StringBuilder();
        for (int i = 0; i < parts.length - 1; i++) {
            remainingStringBuilder.append(parts[i]);
            if (i < parts.length - 2) {
                remainingStringBuilder.append(" ");
            }
        }
        String candidate_name = remainingStringBuilder.toString();
        context.write(new Text("Candidate: "+candidate_name), new Text(score));
        context.write(new Text("Party: "+party_name), new Text(score));
  }
}