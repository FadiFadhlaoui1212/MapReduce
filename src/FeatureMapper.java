import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FeatureMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private final static Text countKey = new Text("count");

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] columns = line.split(","); // Séparateur des colonnes
        if (columns[0].equals("t") && columns[columns.length - 1].equals("won")) {
            context.write(countKey, one); // Émet "count" -> 1
        }
    }
}
