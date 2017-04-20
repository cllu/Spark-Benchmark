package me.cllu.benchmark.utils;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class BatchLineReader implements Closeable {

    private BufferedReader br;
    private String line;

    public BatchLineReader(String path) throws IOException {
        br = new BufferedReader(new InputStreamReader(new FileInputStream(path), "UTF-8"));
        line = br.readLine();
    }
    /**
     * read a batch of N lines
     * return null if we've reached the end of the file.
     */
    public List<String> readBatch(int N) {
        List<String> answer = new ArrayList<>();
        while (line != null && answer.size() < N) {
            answer.add(line);
            try {
                line = br.readLine();
            } catch (IOException e) {
                e.printStackTrace();
                return null;
            }
        }
        if (answer.size() == 0)
            return null;
        return answer;
    }

    @Override
    public void close() {
        try {
            br.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
