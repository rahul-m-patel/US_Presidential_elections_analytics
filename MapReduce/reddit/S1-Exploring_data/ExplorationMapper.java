import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class ExplorationMapper
    extends Mapper<LongWritable, Text, Text, IntWritable> {
  
    private StringBuilder sentenceBuilder = new StringBuilder();
    
    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
        String line = value.toString();
        if (line.contains("Date:")) {    
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

            if (fields.length >= 2) {
                String dateField = fields[0].trim();
                String otherField = fields[1].trim();
    
                if (dateField.startsWith("Date:")) {
                    String date = dateField.length() > "Date:".length() ? dateField.substring("Date:".length() + 1).trim() : "";
    
                    String data = "";
                    if (otherField.contains("Title:")) {
                        data = otherField.length() > "Title:".length() ? otherField.substring("Title:".length() + 1).trim() : "";
                    } else if (otherField.contains("Content:")) {
                        data = otherField.length() > "Content:".length() ? otherField.substring("Content:".length() + 1).trim() : "";
                    } else if (otherField.contains("Comment:")) {
                        data = otherField.length() > "Comment:".length() ? otherField.substring("Comment:".length() + 1).trim() : "";
                    }
    
                    context.write(new Text("Date:" + date), new IntWritable(1));
                    context.write(new Text("Data:" + data), new IntWritable(1));
                }
            }
        }
    
        // Clear the sentence builder for the next sentence
        sentenceBuilder.setLength(0);
    }
    
}