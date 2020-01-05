
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;


public class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable one  = new IntWritable(1);
    private Text word = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        super.map(key, value, context);
        String line = value.toString();
        StringTokenizer tokens = new StringTokenizer(line);

        while(tokens.hasMoreTokens()){
            word.set(tokens.nextToken());
            context.write(word,one);
        }

    }
}