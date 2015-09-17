/**
 * Created by kallurivenkatesh on 9/16/15.
 */
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Odd_Mapper extends Mapper<Object, Text, Text, IntWritable>{
    private final Text PART = new Text("item");
    private IntWritable EACH = new IntWritable();

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] numbers = value.toString().split(" ");
        for (String str : numbers) {
            EACH.set(Integer.parseInt(str));
            context.write(PART, EACH);
        }
    }

}