import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class TokenizeMapper
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
            if(fields.length >= 2){
                String[] tags = fields[1].trim().split(":");
                if(tags.length>=2){
                    String line = tags[1].trim();
                    String[] words = line.split(" ");
                    Pattern pattern = Pattern.compile("[^a-zA-Z0-9]+");
                    for (String word : words) {
                        Matcher matcher = pattern.matcher(word);
                        String cleanedWord = matcher.replaceAll("");
                        String lowercaseWord = cleanedWord.toLowerCase();
                        context.write(new Text(lowercaseWord), new IntWritable(1));
                    }
                }
                
            }
            
        }

        // Clear the sentence builder for the next sentence
        sentenceBuilder.setLength(0);
    }
}