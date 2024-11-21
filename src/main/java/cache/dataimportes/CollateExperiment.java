package cache.dataimportes;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;

/**
 *
 * @author Manjunath Kustagi
 */
public class CollateExperiment {

    static final String KEYSPACE = "cache";
    static final String EXPERIMENT_TABLE = "experiment";

    static final String EXPERIMENT_SCHEMA = String.format("CREATE TABLE %s.%s (\n"
            + "    key int,\n"
            + "    id bigint,\n"
            + "    distance int,\n"
            + "    library text,\n"
            + "    investigator text,\n"
            + "    annotation text,\n"
            + "    method text,\n"
            + "    readlength text,\n"
            + "    attributes map<text, text>,\n"
            + "    PRIMARY KEY ((key, id, distance), library, investigator, annotation, method, readlength)\n"
            + ") WITH CLUSTERING ORDER BY (library ASC, investigator ASC, annotation ASC, method ASC, readlength ASC)", KEYSPACE, EXPERIMENT_TABLE);

    static final String EXPERIMENT_INSERT_STMT = String.format("INSERT INTO %s.%s ("
            + "key, id, distance, library, investigator, annotation, method, readlength, attributes"
            + ") VALUES ("
            + "?, ?, ?, ?, ?, ?, ?, ?, ?"
            + ")", KEYSPACE, EXPERIMENT_TABLE);

    static {
        LogManager.getLogManager().reset();
        Logger globalLogger = Logger.getLogger(java.util.logging.Logger.GLOBAL_LOGGER_NAME);
        globalLogger.setLevel(java.util.logging.Level.WARNING);
    }

    public static void main(String[] args) {
        try {
            String directory = args[0];
            System.out.println("Invoked callable..: experiment.txt");
            CQLSSTableWriter.Builder builder = CQLSSTableWriter.builder();
            builder.inDirectory(directory + "/cache/experiment")
                    .forTable(EXPERIMENT_SCHEMA)
                    .using(EXPERIMENT_INSERT_STMT)
                    .withPartitioner(new Murmur3Partitioner());
            CQLSSTableWriter experiment_writer = builder.build();

            BufferedReader br
                    = new BufferedReader(new FileReader(new File(directory + "/experiment.txt")));

            long count = 0;
            List<String> keys = new ArrayList<>();
            String line = br.readLine();
            String[] tokens = line.split("\t");
            keys.addAll(Arrays.asList(tokens));
            
            while ((line = br.readLine()) != null) {
                tokens = line.split("\t");
                int i = 0;
                Integer key = Integer.parseInt(tokens[0]);
                i++;
                Long id = Long.parseLong(tokens[1]);
                i++;
                Integer distance = Integer.parseInt(tokens[2]);
                i++;
                String library = tokens[3].trim();
                i++;
                String investigator = tokens[4].trim();
                i++;
                String annotation = tokens[5].trim();
                i++;
                String method = tokens[6].trim();
                i++;
                String readlength = tokens[7].trim();
                i++;

                Map<String, String> attributes = new HashMap<>();
                
                for (;i < tokens.length; i++) {
                    attributes.put(keys.get(i), tokens[i]);
                }
                
                experiment_writer.addRow(key, id, distance, library, investigator, annotation, method, readlength, attributes);
                if (count % 10 == 0) {
                    System.out.println("Uploaded " + count + " experiments in file: experiment.txt");
                }
                count++;
            }

            br.close();
            experiment_writer.close();

        } catch (FileNotFoundException ex) {
            Logger.getLogger(CollateExperiment.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(CollateExperiment.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
