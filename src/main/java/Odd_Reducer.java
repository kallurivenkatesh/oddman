/**
 * Created by kallurivenkatesh on 9/16/15.
 */
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class Odd_Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    public void reduce(Text text, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int a = 0;
        for (IntWritable value : values) {
            a ^= value.get();
        }
        context.write(text, new IntWritable(a));
    }

}
