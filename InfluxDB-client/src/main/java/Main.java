import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.WriteApi;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import org.jetbrains.annotations.NotNull;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Main {

    private static final char[] token = "iHrZtzcEfRCA2TTgWTTMPlhVbyvLZov78L9cKzZp3eB_VcZOOXm10DEzpcDAcv35txGJU2wo9bNM0pLva0F5fg==".toCharArray();
    private static final String org = "testorg";
    private static final String bucket = "testbucket";

    private static final List<String> recordBatch = new ArrayList<>();
    private static final Map<String, String> filePathNameMap = new HashMap<>();
    private static String delimiter;
//    private static WriteApi writeApi;

    public static void main(String[] args) {
        ingestCSV(args);
    }


    private static void ingestCSV(String[] args) {
        File sourceDirectory = new File(args[0]);

        if (args.length > 1) {
            if (args[1] == null) {
                System.out.println("Defaulting to sep=\" \"");
                delimiter = " ";
            } else {
                delimiter = args[1];
            }
        } else {
            System.out.println("Defaulting to sep=\" \"");
            delimiter = " ";
        }


        List<File> csvFiles = getCsvFiles(sourceDirectory, "");

        int maxNumberOfConcurrentThreads = 90;
        int sleepTime = 2000;

        List<Thread> workerThreads = new ArrayList<>();

        Stopwatch.setInitialStartTime();
        for (File csvFile : csvFiles) {

            if (workerThreads.size() < maxNumberOfConcurrentThreads) {

                Thread thread = new Thread(() -> {
                    InfluxDBClient influxDBClient = InfluxDBClientFactory.create("http://localhost:8086", token, org, bucket);
                    WriteApi writeApi = influxDBClient.makeWriteApi();
                    writeCsvDataToInfluxDB(csvFile, writeApi);
                    influxDBClient.close();
                });
                workerThreads.add(thread);
                thread.start();

            } else {
                while (maxNumberOfConcurrentThreads == workerThreads.size()) {

                    ArrayList<Thread> deadThreads = getDeadThreads(workerThreads);

                    if (!deadThreads.isEmpty()) {
                        workerThreads.removeAll(deadThreads);
                    } else {
                        try {
                            Thread.sleep(sleepTime);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        }

        while (!workerThreads.isEmpty()) {
            ArrayList<Thread> deadThreads = getDeadThreads(workerThreads);
            if (!deadThreads.isEmpty()) {
                workerThreads.removeAll(deadThreads);
            } else {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }



        Stopwatch.setEndTime();

        System.out.println("Time to ingest: " + Stopwatch.getTime());
    }

    @NotNull
    private static ArrayList<Thread> getDeadThreads(List<Thread> workerThreads) {
        ArrayList<Thread> deadThreads = new ArrayList<>();
        for (Thread workerThread : workerThreads) {
            if (!workerThread.isAlive())
                deadThreads.add(workerThread);
        }
        return deadThreads;
    }


    private static void writeCsvDataToInfluxDB(File file, WriteApi writeApi) {
        if (!file.exists()) {
            System.out.println("File: " + file.getAbsolutePath() + " does not exist");
            return;
        }

        try {
            readFileToInflux(file, writeApi);
        } catch (Exception e) {
            System.out.println("Error reading file: " + file.getAbsolutePath() + e.getMessage());
        }
    }


    private static void readFileToInflux(File file, WriteApi writeApi) throws Exception {
        FileReader fileReader = new FileReader(file);
        BufferedReader bufferedReader = new BufferedReader(fileReader);

        String measurement = filePathNameMap.get(file.getAbsolutePath());

        String fileLine;
        // Run through file
        while ((fileLine = bufferedReader.readLine()) != null) {
            String[] splitLine = fileLine.split(delimiter);
            if (splitLine.length == 2) {
                writeDataPointToInflux(measurement, splitLine[0].trim(), splitLine[1].trim(), writeApi);
            } else {
                writeDataPointToInflux(splitLine[0].trim(), splitLine[1].trim(), splitLine[2].trim(), writeApi);
            }
        }
    }


    private static void writeDataPointToInflux(String measurement, String timestamp, String value, WriteApi writeApi) {
        String point = Point.measurement(measurement)
                .addField("value", Double.parseDouble(value))
                .time(Long.parseLong(timestamp), WritePrecision.MS).toLineProtocol();

        // Do batching
        if (recordBatch.size() == 5000) {
            writeApi.writeRecords(WritePrecision.MS, recordBatch);
            recordBatch.clear();
        } else {
            recordBatch.add(point);
        }
    }


    private static List<File> getCsvFiles(File sourceDirectory, String filePath) {
        List<File> csvFiles = new ArrayList<>();

        File[] subFiles = sourceDirectory.listFiles();

        if (subFiles == null) {
            if (sourceDirectory.isFile())
                csvFiles.add(sourceDirectory);
            return csvFiles;
        }

        for (File file : subFiles) {
            if (file.isDirectory()) {
                csvFiles.addAll(getCsvFiles(file, filePath + "/" + file.getName()));
            } else if (file.isFile()) {
                filePathNameMap.put(file.getAbsolutePath(), filePath + "/" + file.getName());
                csvFiles.add(file);
            }
        }
        return csvFiles;
    }
}
