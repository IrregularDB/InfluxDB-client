import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.WriteApi;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.*;

public class Main {

    private static final char[] token = "NrWTfM9a5Pmg-45DOUJawRKwNrUVQGFUBSNBKqyM1-jYuVsmNBKIXHMOI2ztNbluXG79wEBjXLvI_dbnkhKQbg==".toCharArray();
    private static final String org = "p10";
    private static final String bucket = "irregularbucket";

    private static final List<String> recordBatch = new ArrayList<>();

    private static String delimiter;
    private static final Map<String, String> filePathNameMap = new HashMap<>();
    private static WriteApi writeApi;


    public static void main(String[] args) {
        ingestCSV(args);
    }


    private static void ingestCSV(String[] args) {
        File sourceDirectory = new File(args[0]);

        if (args.length > 1){
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

        InfluxDBClient influxDBClient = InfluxDBClientFactory.create("http://localhost:8086", token, org, bucket);

        writeApi = influxDBClient.makeWriteApi();

        List<File> csvFiles = getCsvFiles(sourceDirectory, "");

        System.out.println("Printing CSV files:");
        System.out.println(csvFiles);

        Stopwatch.setInitialStartTime();
        writeCsvDataToInfluxDB(csvFiles);
        Stopwatch.setEndTime();

        System.out.println("Time to ingest: " + Stopwatch.getTime());
        influxDBClient.close();
    }


    private static void writeCsvDataToInfluxDB(List<File> csvFiles) {
        for (File file : csvFiles){
            if (!file.exists()){
                System.out.println("File: " + file.getAbsolutePath() + " does not exist");
                continue;
            }

            try {
                readFileToInflux(file);
            } catch (Exception e){
                System.out.println("Error reading file: " + file.getAbsolutePath() + e.getMessage());
            }
        }
    }


    private static void readFileToInflux(File file) throws Exception {
        FileReader fileReader = new FileReader(file);
        BufferedReader bufferedReader = new BufferedReader(fileReader);

        String measurement = filePathNameMap.get(file.getAbsolutePath());

        String fileLine;
        // Run through file
        while ((fileLine = bufferedReader.readLine()) != null){
            String[] splitLine = fileLine.split(delimiter);
            if (splitLine.length == 2){
                writeDataPointToInflux(measurement, splitLine[0].trim(), splitLine[1].trim());
            } else {
                writeDataPointToInflux(splitLine[0].trim(), splitLine[1].trim(), splitLine[2].trim());
            }
        }
    }


    private static void writeDataPointToInflux(String measurement, String timestamp, String value) {
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


    private static List<File> getCsvFiles(File sourceDirectory, String filePath){
        List<File> csvFiles = new ArrayList<>();

        File[] subFiles = sourceDirectory.listFiles();

        if (subFiles == null){
            if (sourceDirectory.isFile())
                csvFiles.add(sourceDirectory);
            return csvFiles;
        }

        for (File file : subFiles){
            if (file.isDirectory()){
                csvFiles.addAll(getCsvFiles(file, filePath + "/" + file.getName()));
            } else if (file.isFile()) {
                filePathNameMap.put(file.getAbsolutePath(), filePath + "/" + file.getName());
                csvFiles.add(file);
            }
        }
        return csvFiles;
    }
}
