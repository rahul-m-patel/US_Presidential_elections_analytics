import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class RecordsByDateMapper
    extends Mapper<LongWritable, Text, Text, IntWritable> {
  
    private StringBuilder sentenceBuilder = new StringBuilder();
    
    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
        String line = value.toString();
        if (line.contains("Date:") || line.contains("Source:")) {    
            emitSentence(context); // Emit the previous sentence if any
            sentenceBuilder.append(line.trim()); // Start a new sentence
        } else {
            sentenceBuilder.append(" ").append(line.trim());
        }
        
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // Emit the last sentence before the cleanup
        emitSentence(context);
    }

    private void emitSentence(Context context) throws IOException, InterruptedException {
        String sentence = sentenceBuilder.toString().trim();
        if (!sentence.isEmpty()) {
            String[] fields = sentence.split("\\|");
            String date = fields[0].trim().substring("Date:".length()).trim();

            context.write(new Text(date), new IntWritable(1));
        }

        // Clear the sentence builder for the next sentence
        sentenceBuilder.setLength(0);
    }
}