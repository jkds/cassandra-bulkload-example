
package bulkload;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;
import org.slf4j.Logger;
import org.supercsv.io.CsvListReader;
import org.supercsv.prefs.CsvPreference;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Usage: java bulkload.Loader <csv_dir_location>
 */
public class Loader {

    private static final Logger log = org.slf4j.LoggerFactory.getLogger(Loader.class);

    /**
     * Default output directory
     */
    public static final String DEFAULT_OUTPUT_DIR = "./data";
    static String filename;
    static String csv_dir_location;

    /**
     * Keyspace name
     */
    public static final String KEYSPACE = "staging";
    /**
     * Table name
     */
    public static final String TABLE = "mykeyspace";


    /**
     * Schema for bulk loading table.
     * It is important not to forget adding keyspace name before table name,
     * otherwise CQLSSTableWriter throws exception.
     */
    public static final String SCHEMA = String.format("CREATE TABLE %s.%s (" +
            "id1 text, " +
            "id2 text, " +
            "type text, " +
            "units text, " +
            "date text, " +
            "year text, " +
            "month text, " +
            "day text, " +
            "time text, " +
            "sample text, " +
            "primary key ((year, month, day), id2, time)) ", KEYSPACE, TABLE);

    /**
     * INSERT statement to bulk load.
     * It is like prepared statement. Fill in place holder for each data.
     */
    public static final String INSERT_STMT = String.format("INSERT INTO %s.%s (" +
            "id1, id2, type, units, date, year, month, day, time, sample" +
            ") VALUES (" +
            "?, ?, ?, ?, ?, ?, ?, ?, ?, ?" +
            ")", KEYSPACE, TABLE);

    // list csv files in given directory
    public static List listOfFiles() {
        List<String> results = new ArrayList<String>();

        File[] files = new File(csv_dir_location).listFiles(new FileFilter() {
            public boolean accept(File file) {
                return file.getName().endsWith(".csv") && file.isFile();
            }
        });

        for (File file : files) {
             results.add(file.getAbsolutePath());
        }
        return results;
    }

    public static void main(String[] args) {
        if (args.length == 0) {
            System.out.println("usage: java bulkload.Loader <csv_dir_location>");
            return;
        }

        csv_dir_location = args[0];

        log.info("CSV Dir = {}", csv_dir_location);


        List list = listOfFiles();
        int size = list.size();

        log.info("# of Files to Process = {}", size);

        // magic!
        Config.setClientMode(true);

        // Create output directory that has keyspace and table name in the path
        File outputDir = new File(DEFAULT_OUTPUT_DIR + File.separator + KEYSPACE + File.separator + TABLE);
        if (!outputDir.exists() && !outputDir.mkdirs()) {
            throw new RuntimeException("Cannot create output directory: " + outputDir);
        }

        // Prepare SSTable writer
        CQLSSTableWriter.Builder builder = CQLSSTableWriter.builder();
        // set output directory
        builder.inDirectory(outputDir)
                // set target schema
                .forTable(SCHEMA)
                        // set CQL statement to put data
                .using(INSERT_STMT)

                .withBufferSizeInMB(128)
                        // set partitioner if needed
                        // default is Murmur3Partitioner so set if you use different one.
                .withPartitioner(new Murmur3Partitioner());

        long start = System.currentTimeMillis();

        for (int i = 0; i < size; i++) {
            //CQLSSTableWriter writer = builder.build();

            filename = list.get(i).toString();
            log.debug("Processing file {} " + filename);
            try (BufferedReader reader = new BufferedReader(new FileReader(filename));
                CsvListReader csvReader = new CsvListReader(reader, CsvPreference.STANDARD_PREFERENCE)) {
                CQLSSTableWriter writer = builder.build();
                csvReader.getHeader(false);
                // Write to SSTable while reading data
                List<String> line;
                long noOfLines = 0;
                while ((line = csvReader.read()) != null) {
                    if (line.size() == 10) {
                        writer.addRow(line.toArray(new Object[]{line.size()}));
                        noOfLines++;
                        if (noOfLines % 100000 == 0) {
                            log.debug("File {}, {} lines written", filename, noOfLines);
                        }
                    } else {
                        log.warn("Line with {} columns does not conform, skipping", line.size());
                    }
                }
                log.info("Completed writing of {}", filename);
                writer.close();
            } catch (Exception e) {
                e.printStackTrace();
            }

        }//for loop

        log.info("Written sstables files in {} seconds", ((System.currentTimeMillis()-start)/1000));

    }


}
