import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class RemoveUnwantedColsMapper
    extends Mapper<LongWritable, Text, Text, NullWritable> {
  
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    
        String[] columns = value.toString().split(",");

        // Keeping only desired columns and removing rows with missing values
        if(columns[0].length()>0 && columns[1].length()>0 && columns[2].length()>0 && columns[9].length()>0 && columns[12].length()>0 &&
        columns[13].length()>0 && columns[18].length()>0 && columns[39].length()>0 && columns[41].length()>0 && columns[42].length()>0 && columns[43].length()>0){
          String result = columns[0] + "," + columns[1] + "," + columns[2] + "," + columns[9] + "," + columns[12] + "," + columns[13] + "," + columns[18]
          + "," + columns[39] + "," + columns[41] + "," + columns[42] + "," + columns[43]; 

          context.write(new Text(result), NullWritable.get());
        }
        
  }
}