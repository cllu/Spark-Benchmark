package me.cllu.benchmark;

import me.cllu.benchmark.utils.BatchLineReader;
import org.json.JSONObject;

import java.io.*;
import java.util.List;

/**
 * Java parallel stream version
 */
public class WikidataExtractorStream {
  private static final int BATCH_SIZE = 10_000;

  public static void main(String[] args) {
    String inputPath = args[0];
    String outputPath = "data/labels-stream.txt";

    long taskStartTime = System.currentTimeMillis();
    try (BatchLineReader br = new BatchLineReader(inputPath);
         BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(outputPath))))
    ) {
      for (List<String> batch = br.readBatch(BATCH_SIZE); batch != null; batch = br.readBatch(BATCH_SIZE)) {
        batch.parallelStream().forEach(line -> {
              if (line.equals("[") || line.equals("]")) {
                return;
              } else if (line.endsWith(",")) {
                line = line.substring(0, line.length() - 1);
              }

              try {
                JSONObject entity = new JSONObject(line);
                JSONObject labels = entity.getJSONObject("labels");
                if (!labels.isNull("en")) {
                  line = entity.getString("id") + "\t" + labels.getJSONObject("en").getString("value") + "\n";
                  synchronized (bw) {
                    bw.write(line);
                  }
                }
              } catch (IOException e) {
                e.printStackTrace();
              }
            }
        );
      }
    } catch (IOException e) {
      e.printStackTrace();
    }

    long taskDuration = System.currentTimeMillis() - taskStartTime;
    System.out.println(taskDuration);
  }
}
