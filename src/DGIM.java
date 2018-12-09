package dgim;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DGIM {
  public static int N = 50; // window size
  // ceil(log N)  * r
  public static int n = 6; // size of power of max bucket
  public static int r = 1; // max number of buckets for one size

  public static class DGIMReducer extends Reducer<Text, Text, IntWritable, IntWritable> {
    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
      int i, j, k, time_counter = Integer.parseInt(key.toString());
      int[][] buckets = new int[DGIM.n + 1][DGIM.r + 1];
      int[] bucket_counter = new int[DGIM.n + 1];

      // initialize buckets
      for (i = 0; i <= DGIM.n; ++i) {
        bucket_counter[i] = 0;
        for (j = 0; j <= DGIM.r; ++j) buckets[i][j] = -1;
      }
      // load buckets
      Configuration conf = context.getConfiguration();
      if (!conf.get("bucket").equals("init_bucket")) {
        String[] data = conf.get("bucket").split("\n");
        for (i = 0; i < data.length; ++i) {
          String[] entry = data[i].split("\t");
          int power = Integer.parseInt(entry[0]), end = Integer.parseInt(entry[1]);
          j = 0;
          while (buckets[power][j] != -1) ++j;
          buckets[power][j] = end;
          bucket_counter[power]++;
        }
      }

      // streaming
      for (Text value : values) {
        String[] data_stream = value.toString().split(" ");
        for (i = 0; i < data_stream.length; ++i, ++time_counter) {
          if (data_stream[i].equals("0")) continue;
          int be_merged = 0;
          while (bucket_counter[be_merged] >= DGIM.r) ++be_merged;
          for (j = be_merged; j >= 0; --j) {
            for (k = DGIM.r; k >= 1; --k) {
              buckets[j][k] = buckets[j][k - 1];
            }
          }
          buckets[0][0] = time_counter;

          for (j = 1; j <= be_merged; ++j) buckets[j][0] = buckets[j - 1][DGIM.r - 1];
          for (j = be_merged; j >= 1; --j) {
            buckets[j - 1][DGIM.r] = -1;
            buckets[j - 1][DGIM.r - 1] = -1;
          }

          bucket_counter[be_merged]++;
          for (j = 0; j < be_merged; ++j) bucket_counter[j]--;

          for (j = DGIM.n; j >= 0; --j) {
            for (k = DGIM.r; k >= 0; --k) {
              if (buckets[j][k] == -1) continue;
              if (time_counter - buckets[j][k] > DGIM.N) {
                buckets[j][k] = -1;
                bucket_counter[j]--;
              } else break;
            }
          }
        }
      }

      // output buckets
      for (i = 0; i <= DGIM.n; ++i) {
        for (j = 0; j <= DGIM.r; ++j)
          if (buckets[i][j] != -1)
            context.write(new IntWritable(i), new IntWritable(buckets[i][j]));
      }
    }
  }

  public static void run(String input, String output, String bucket) throws Exception {
    Configuration conf = new Configuration();
    conf.set("bucket", bucket);

    Job job = new Job(conf, "DGIM");
    job.setJarByClass(DGIM.class);

    job.setInputFormatClass(KeyValueTextInputFormat.class);

    // Identity Mapper
    job.setReducerClass(DGIMReducer.class);

    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(IntWritable.class);

    FileInputFormat.addInputPath(job, new Path(input));
    FileOutputFormat.setOutputPath(job, new Path(output));

    job.waitForCompletion(true);
  }

  public static String readBucket(final String input) throws IOException {
    FileSystem fs = FileSystem.get(new Configuration());
    FileStatus[] status = fs.listStatus(new Path(input));
    String ret = "", res;
    for (FileStatus f : status) {
      if (!f.isFile()) continue;
      BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(f.getPath())));
      while ((res = reader.readLine()) != null) {
        ret += res + "\n";
      }
      reader.close();
    }
    return ret;
  }

  public static void main(String[] args) throws Exception {
    final String inputPath = args[0] + "/", outputPath = args[1] + "/";
    final int iters = 10;

    String bucket = "init_bucket";
    for (int i = 0; i < iters; ++i) {
      String input = inputPath + "data" + Integer.toString(i) + ".txt",
          output = outputPath + Integer.toString(i);
      run(input, output, bucket);
      bucket = readBucket(output);
    }
  }
}
