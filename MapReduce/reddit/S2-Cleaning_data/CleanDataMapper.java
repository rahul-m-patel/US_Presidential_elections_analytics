import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class CleanDataMapper
    extends Mapper<LongWritable, Text, Text, NullWritable> {
  
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

    private String removeRemovedAndDeleted(String input) {
        // Remove "[removed]" and "[deleted]" using a regular expression
        String regexRemovedAndDeleted = "\\[removed\\]|\\[deleted\\]";
        return input.replaceAll(regexRemovedAndDeleted, "");
    }

    private String removeEmojis(String input) {
        // Remove emojis using a regular expression
        String regexEmojis = "[\\x{1F600}-\\x{1F64F}\\x{1F300}-\\x{1F5FF}\\x{1F680}-\\x{1F6FF}\\x{1F700}-\\x{1F77F}\\x{1F780}-\\x{1F7FF}\\x{1F800}-\\x{1F8FF}\\x{1F900}-\\x{1F9FF}\\x{1FA00}-\\x{1FA6F}\\x{2600}-\\x{26FF}\\x{2700}-\\x{27BF}\\x{2B05}\\x{2B06}\\x{2B07}\\x{2934}\\x{2935}\\x{25AA}\\x{25AB}\\x{25FE}\\x{25FD}\\x{25FB}\\x{25FC}\\x{200D}\\x{23E9}\\x{23EA}\\x{23EB}\\x{23EC}\\x{23F0}\\x{23F3}\\x{26A0}\\x{26D4}]";
        return input.replaceAll(regexEmojis, "");
    }

    private String removeLinks(String input) {
        // Remove website links using a regular expression
        String regexLinks = "(http[s]?://|www\\.)\\S+";
        return input.replaceAll(regexLinks, "");
    }

    private String cleanAndFilter(String input) {
        return removeRemovedAndDeleted(removeEmojis(removeLinks(input)));
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
                    String cleaned = "";
                    int index = -1;
                    String [] data_type = {"Title:","Content:","Comment:"};
                    if (otherField.contains("Title:")) {
                        data = otherField.length() > "Title:".length() ? otherField.substring("Title:".length() + 1).trim() : "";
                        index = 0;
                    } else if (otherField.contains("Content:")) {
                        data = otherField.length() > "Content:".length() ? otherField.substring("Content:".length() + 1).trim() : "";
                        index = 1;
                    } else if (otherField.contains("Comment:")) {
                        data = otherField.length() > "Comment:".length() ? otherField.substring("Comment:".length() + 1).trim() : "";
                        index = 2;
                    }
                    if(data.length()>0){
                        cleaned = cleanAndFilter(data);
                    }
                    if(cleaned.length()>0){
                        String final_string = dateField+" | "+data_type[index]+" "+cleaned;
                        context.write(new Text(final_string), NullWritable.get());
                    }
                    
                    
                }
            }
        }
    
        // Clear the sentence builder for the next sentence
        sentenceBuilder.setLength(0);
    }
}