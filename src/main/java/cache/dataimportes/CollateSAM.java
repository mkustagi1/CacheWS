package cache.dataimportes;

import com.datastax.driver.core.utils.UUIDs;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;

/**
 *
 * @author Manjunath Kustagi
 */
public class CollateSAM {

    static final String KEYSPACE = "cache";
    static final String TA_TABLE = "transcriptalignment";
    static final String TAT_TABLE = "transcriptalignmenttranspose";
    static final String TA1_TABLE = "transcriptalignment1mismatch";
    static final String TAT1_TABLE = "transcriptalignment1mismatchtranspose";
    static final String TA2_TABLE = "transcriptalignment2mismatch";
    static final String TAT2_TABLE = "transcriptalignment2mismatchtranspose";
    static final String UMR_TABLE = "unmappedreads";
    static final String READS_TABLE = "reads";

    static final String TATSS_TABLE = "transcriptalignmenttss";
    static final String TATSST_TABLE = "transcriptalignmenttsstranspose";

    static final String TATSS_SCHEMA = String.format("CREATE TABLE %s.%s (\n"
            + "    key int,\n"
            + "    transcriptid bigint,\n"
            + "    reversecomplement boolean,\n"
            + "    count bigint,\n"
            + "    id timeuuid,\n"
            + "    readid timeuuid,\n"
            + "    score double,\n"
            + "    startcoordinate bigint,\n"
            + "    stopcoordinate bigint,\n"
            + "    PRIMARY KEY (key, transcriptid, reversecomplement, count, id)\n"
            + ") WITH CLUSTERING ORDER BY (transcriptid ASC, reversecomplement ASC, count ASC, id ASC)", KEYSPACE, TATSS_TABLE);

    static final String TATSS_INSERT_STMT = String.format("INSERT INTO %s.%s ("
            + "key, transcriptid, reversecomplement, count, id, readid, score, startcoordinate, stopcoordinate"
            + ") VALUES ("
            + "?, ?, ?, ?, ?, ?, ?, ?, ?"
            + ")", KEYSPACE, TATSS_TABLE);

    static final String TATSST_SCHEMA = String.format("CREATE TABLE %s.%s (\n"
            + "    key bigint,\n"
            + "    readid timeuuid,\n"
            + "    transcriptid bigint,\n"
            + "    PRIMARY KEY (key, readid, transcriptid)\n"
            + ") WITH CLUSTERING ORDER BY (readid ASC, transcriptid ASC)", KEYSPACE, TATSST_TABLE);

    static final String TATSST_INSERT_STMT = String.format("INSERT INTO %s.%s ("
            + "key, readid, transcriptid"
            + ") VALUES ("
            + "?, ?, ?"
            + ")", KEYSPACE, TATSST_TABLE);

    static final String TA_SCHEMA = String.format("CREATE TABLE %s.%s (\n"
            + "    experimentid bigint,\n"
            + "    transcriptid bigint,\n"
            + "    reversecomplement boolean,\n"
            + "    id timeuuid,\n"
            + "    readcount bigint,\n"
            + "    readid timeuuid,\n"
            + "    score double,\n"
            + "    startcoordinate bigint,\n"
            + "    stopcoordinate bigint,\n"
            + "    PRIMARY KEY (experimentid, transcriptid, reversecomplement, id)\n"
            + ") WITH CLUSTERING ORDER BY (transcriptid ASC, reversecomplement ASC, id ASC)", KEYSPACE, TA_TABLE);

    static final String TAT_SCHEMA = String.format("CREATE TABLE %s.%s (\n"
            + "    experimentid bigint,\n"
            + "    readid timeuuid,\n"
            + "    transcriptid bigint,\n"
            + "    PRIMARY KEY (experimentid, readid, transcriptid)\n"
            + ") WITH CLUSTERING ORDER BY (readid ASC, transcriptid ASC)", KEYSPACE, TAT_TABLE);

    static final String TA_INSERT_STMT = String.format("INSERT INTO %s.%s ("
            + "experimentid, transcriptid, reversecomplement, id, readcount, readid, score, startcoordinate, stopcoordinate"
            + ") VALUES ("
            + "?, ?, ?, ?, ?, ?, ?, ?, ?"
            + ")", KEYSPACE, TA_TABLE);

    static final String TAT_INSERT_STMT = String.format("INSERT INTO %s.%s ("
            + "experimentid, readid, transcriptid"
            + ") VALUES ("
            + "?, ?, ?"
            + ")", KEYSPACE, TAT_TABLE);

    static final String TA1_SCHEMA = String.format("CREATE TABLE %s.%s (\n"
            + "    experimentid bigint,\n"
            + "    transcriptid bigint,\n"
            + "    reversecomplement boolean,\n"
            + "    mismatchtype text,\n"
            + "    id timeuuid,\n"
            + "    mismatchcoordinate int,\n"
            + "    readcount bigint,\n"
            + "    readid timeuuid,\n"
            + "    score double,\n"
            + "    startcoordinate bigint,\n"
            + "    stopcoordinate bigint,\n"
            + "    variant text,\n"
            + "    PRIMARY KEY (experimentid, transcriptid, reversecomplement, mismatchtype, id)\n"
            + ") WITH CLUSTERING ORDER BY (transcriptid ASC, reversecomplement ASC, mismatchtype ASC, id ASC)", KEYSPACE, TA1_TABLE);

    static final String TAT1_SCHEMA = String.format("CREATE TABLE %s.%s (\n"
            + "    experimentid bigint,\n"
            + "    readid timeuuid,\n"
            + "    transcriptid bigint,\n"
            + "    PRIMARY KEY (experimentid, readid, transcriptid)\n"
            + ") WITH CLUSTERING ORDER BY (readid ASC, transcriptid ASC)", KEYSPACE, TAT1_TABLE);

    static final String TA1_INSERT_STMT = String.format("INSERT INTO %s.%s ("
            + "experimentid, transcriptid, reversecomplement, mismatchtype, id, mismatchcoordinate, readcount, readid, score, startcoordinate, stopcoordinate, variant"
            + ") VALUES ("
            + "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?"
            + ")", KEYSPACE, TA1_TABLE);

    static final String TAT1_INSERT_STMT = String.format("INSERT INTO %s.%s ("
            + "experimentid, readid, transcriptid"
            + ") VALUES ("
            + "?, ?, ?"
            + ")", KEYSPACE, TAT1_TABLE);

    static final String TA2_SCHEMA = String.format("CREATE TABLE %s.%s (\n"
            + "    experimentid bigint,\n"
            + "    transcriptid bigint,\n"
            + "    reversecomplement boolean,\n"
            + "    mismatch1type text,\n"
            + "    mismatch2type text,\n"
            + "    id timeuuid,\n"
            + "    mismatch1coordinate int,\n"
            + "    mismatch2coordinate int,\n"
            + "    readcount bigint,\n"
            + "    readid timeuuid,\n"
            + "    score double,\n"
            + "    startcoordinate bigint,\n"
            + "    stopcoordinate bigint,\n"
            + "    variant1 text,\n"
            + "    variant2 text,\n"
            + "    PRIMARY KEY (experimentid, transcriptid, reversecomplement, mismatch1type, mismatch2type, id)\n"
            + ") WITH CLUSTERING ORDER BY (transcriptid ASC, reversecomplement ASC, mismatch1type ASC, mismatch2type ASC, id ASC)", KEYSPACE, TA2_TABLE);

    static final String TAT2_SCHEMA = String.format("CREATE TABLE %s.%s (\n"
            + "    experimentid bigint,\n"
            + "    readid timeuuid,\n"
            + "    transcriptid bigint,\n"
            + "    PRIMARY KEY (experimentid, readid, transcriptid)\n"
            + ") WITH CLUSTERING ORDER BY (readid ASC, transcriptid ASC)", KEYSPACE, TAT2_TABLE);

    static final String TA2_INSERT_STMT = String.format("INSERT INTO %s.%s ("
            + "experimentid, transcriptid, reversecomplement, mismatch1type, mismatch2type, id, mismatch1coordinate, "
            + "mismatch2coordinate, readcount, readid, score, startcoordinate, stopcoordinate, variant1, variant2"
            + ") VALUES ("
            + "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?"
            + ")", KEYSPACE, TA2_TABLE);

    static final String TAT2_INSERT_STMT = String.format("INSERT INTO %s.%s ("
            + "experimentid, readid, transcriptid"
            + ") VALUES ("
            + "?, ?, ?"
            + ")", KEYSPACE, TAT2_TABLE);

    static final String UMR_SCHEMA = String.format("CREATE TABLE %s.%s (\n"
            + "    experimentid bigint,\n"
            + "    readid timeuuid,\n"
            + "    PRIMARY KEY (experimentid, readid)\n"
            + ") WITH CLUSTERING ORDER BY (readid ASC)", KEYSPACE, UMR_TABLE);

    static final String UMR_INSERT_STMT = String.format("INSERT INTO %s.%s ("
            + "experimentid, readid"
            + ") VALUES ("
            + "?, ?"
            + ")", KEYSPACE, UMR_TABLE);

    static final String READS_SCHEMA = String.format("CREATE TABLE cache.reads (\n"
            + "    experimentid bigint,\n"
            + "    id timeuuid,\n"
            + "    count bigint,\n"
            + "    sequence text,\n"
            + "    PRIMARY KEY (experimentid, id)\n"
            + ") WITH CLUSTERING ORDER BY (id ASC)", KEYSPACE, READS_TABLE);

    static final String READS_INSERT_STMT = String.format("INSERT INTO %s.%s ("
            + "experimentid, id, count, sequence"
            + ") VALUES ("
            + "?, ?, ?, ?"
            + ")", KEYSPACE, READS_TABLE);

    static {
//        LogManager.getLogManager().reset();
//        Logger globalLogger = Logger.getLogger(java.util.logging.Logger.GLOBAL_LOGGER_NAME);
//        globalLogger.setLevel(java.util.logging.Level.WARNING);
    }

    public static void main(String[] args) {
        try {
            String[] files = new String[]{"xaa", "xab", "xac", "xad", "xae", "xaf",
                "xag", "xah", "xai", "xaj", "xak", "xal", "xam", "xan", "xao", "xap",
                "xaq", "xar", "xas", "xat", "xau", "xav", "xaw", "xax", "xay", "xaz",
                "xba", "xbb", "xbc", "xbd"};

            ExecutorService ex = Executors.newFixedThreadPool(files.length, (Runnable r) -> {
                Thread t = new Thread(r);
                t.setPriority(Thread.MAX_PRIORITY);
                return t;
            });

            List<Callable<Object>> callables = new ArrayList<>();
            final String directory = args[0];
//            final String file = args[1];
            for (String file : files) {
//                CallableReads t = new CallableReads(file, directory);
//                CallableTranscriptAlignmentsTss t = new CallableTranscriptAlignmentsTss(file, directory);
//                CallableTranscriptAlignments t0 = new CallableTranscriptAlignments(file, directory);
//                callables.add(t0);
//                CallableTranscript1Mismatch t = new CallableTranscript1Mismatch(file, directory);
//                CallableTranscript2Mismatches t = new CallableTranscript2Mismatches(file, directory);
                CallableTranscriptUnmappedReads t = new CallableTranscriptUnmappedReads(file, directory);
//                CallableTranscriptAlignmentsTss t = new CallableTranscriptAlignmentsTss(file, directory);
                callables.add(t);
            }

            ex.invokeAll(callables);
            ex.shutdown();
        } catch (InterruptedException ex) {
            Logger.getLogger(CollateSAM.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    static class CallableTranscriptAlignments implements Callable {

        String file;
        String directory;

        public CallableTranscriptAlignments(String file, String directory) {
            this.file = file;
            this.directory = directory;
        }

        @Override
        public Object call() throws Exception {
            try {
                System.out.println("Invoked callable..: " + file);
                CQLSSTableWriter.Builder builder = CQLSSTableWriter.builder();
                builder.inDirectory(directory + "/cache/transcriptalignment")
                        .forTable(TA_SCHEMA)
                        .using(TA_INSERT_STMT)
                        .withPartitioner(new Murmur3Partitioner());
                CQLSSTableWriter ta_writer = builder.build();

                builder = CQLSSTableWriter.builder();
                builder.inDirectory(directory + "/cache/transcriptalignmenttranspose")
                        .forTable(TAT_SCHEMA)
                        .using(TAT_INSERT_STMT)
                        .withPartitioner(new Murmur3Partitioner());
                CQLSSTableWriter tat_writer = builder.build();

                BufferedReader br
                        = new BufferedReader(new FileReader(new File(directory + "/" + file + ".bowtie.sam")));

                long count = 0;
                String line;
                while ((line = br.readLine()) != null) {
                    if (line.startsWith("#") || line.startsWith("Reported")) {
                        continue;
                    }
                    String[] tokens = line.split("\t");
                    if (tokens.length <= 7) {
                        String[] toks = tokens[0].split("_");
                        Long experimentId = Long.parseLong(toks[0]);
                        UUID rid = UUID.fromString(toks[1]);
                        Long readCount = Long.parseLong(toks[2]);
                        Boolean rc = true;
                        if (tokens[1].equals("+")) {
                            rc = false;
                        }

                        UUID id = UUIDs.timeBased();
//                    UUID id = UUIDGenerator.generate();
                        toks = tokens[2].split("_");
                        Long tid = Long.parseLong(toks[1]);
                        Long startCoordinate = Long.parseLong(tokens[3]);
                        Double score = ((Integer) tokens[4].length()).doubleValue();
                        Long stopCoordinate = startCoordinate + score.longValue();
                        ta_writer.addRow(experimentId, tid, rc, id, readCount, rid, score, startCoordinate, stopCoordinate);
                        tat_writer.addRow(experimentId, rid, tid);
                    }
                    if (count % 100000 == 0) {
                        System.out.println("Uploaded " + count + " alignments in file: " + file);
                    }
                    count++;
                }

                br.close();
                ta_writer.close();
                tat_writer.close();
            } catch (IOException | NumberFormatException | InvalidRequestException e) {
                Logger.getLogger(CollateSAM.class.getName()).log(Level.SEVERE, null, e);
            }
            return file;
        }
    }

    static class CallableTranscript1Mismatch implements Callable {

        String file;
        String directory;

        public CallableTranscript1Mismatch(String file, String directory) {
            this.file = file;
            this.directory = directory;
        }

        @Override
        public Object call() throws Exception {
            try {
                System.out.println("Invoked callable..: " + file);
                CQLSSTableWriter.Builder builder = CQLSSTableWriter.builder();
                builder.inDirectory(directory + "/cache/transcriptalignment1mismatch")
                        .forTable(TA1_SCHEMA)
                        .using(TA1_INSERT_STMT)
                        .withPartitioner(new Murmur3Partitioner());
                CQLSSTableWriter ta1_writer = builder.build();

                builder = CQLSSTableWriter.builder();
                builder.inDirectory(directory + "/cache/transcriptalignment1mismatchtranspose")
                        .forTable(TAT1_SCHEMA)
                        .using(TAT1_INSERT_STMT)
                        .withPartitioner(new Murmur3Partitioner());
                CQLSSTableWriter tat1_writer = builder.build();

                BufferedReader br
                        = new BufferedReader(new FileReader(new File(directory + "/" + file + ".bowtie.sam")));

                long count = 0;
                String line;
                while ((line = br.readLine()) != null) {
                    if (line.startsWith("#") || line.startsWith("Reported")) {
                        continue;
                    }
                    String[] tokens = line.split("\t");
                    if (tokens.length >= 8 && tokens[7] != null) {
                        String[] toks = tokens[0].split("_");
                        Long experimentId = Long.parseLong(toks[0]);
                        UUID rid = UUID.fromString(toks[1]);
                        Long readCount = Long.parseLong(toks[2]);
                        assert rid.version() == 1;

                        Boolean rc = true;
                        if (tokens[1].equals("+")) {
                            rc = false;
                        }

                        UUID id = UUIDs.timeBased();
//                    UUID id = UUIDGenerator.generate();
                        assert id.version() == 1;

                        toks = tokens[2].split("_");
                        Long tid = Long.parseLong(toks[1]);
                        Long startCoordinate = Long.parseLong(tokens[3]);
                        Double score = ((Integer) tokens[4].length()).doubleValue();
                        Long stopCoordinate = startCoordinate + score.longValue();
                        Integer mmc1 = 0, mmc2 = 0;
                        String v1 = "", v2 = "";
                        if (tokens[7].contains(",")) {
                        } else {
                            String[] tv = tokens[7].trim().split(":");
                            mmc1 = Integer.parseInt(tv[0]);
                            String[] tv1 = tv[1].split(">");
                            v1 = tv1[1];

//                            System.out.println("rid, id: " + rid.toString() + ", " + id.toString());                            
                            ta1_writer.addRow(experimentId, tid, rc, "S", id, mmc1, readCount, rid, score, startCoordinate, stopCoordinate, v1);
                            tat1_writer.addRow(experimentId, rid, tid);
                        }
                    }
                    if (count % 100000 == 0) {
                        System.out.println("Uploaded " + count + " alignments in file: " + file);
                    }
                    count++;
                }

                br.close();
                ta1_writer.close();
                tat1_writer.close();
            } catch (IOException | NumberFormatException | InvalidRequestException e) {
                Logger.getLogger(CollateSAM.class.getName()).log(Level.SEVERE, null, e);
            }
            return file;
        }
    }

    static class CallableTranscript2Mismatches implements Callable {

        String file;
        String directory;

        public CallableTranscript2Mismatches(String file, String directory) {
            this.file = file;
            this.directory = directory;
        }

        @Override
        public Object call() throws Exception {
            try {
                System.out.println("Invoked callable..: " + file);
                CQLSSTableWriter.Builder builder = CQLSSTableWriter.builder();
                builder.inDirectory(directory + "/cache/transcriptalignment2mismatch")
                        .forTable(TA2_SCHEMA)
                        .using(TA2_INSERT_STMT)
                        .withPartitioner(new Murmur3Partitioner());
                CQLSSTableWriter ta2_writer = builder.build();

                builder = CQLSSTableWriter.builder();
                builder.inDirectory(directory + "/cache/transcriptalignment2mismatchtranspose")
                        .forTable(TAT2_SCHEMA)
                        .using(TAT2_INSERT_STMT)
                        .withPartitioner(new Murmur3Partitioner());
                CQLSSTableWriter tat2_writer = builder.build();

                BufferedReader br
                        = new BufferedReader(new FileReader(new File(directory + "/" + file + ".bowtie.sam")));

                long count = 0;
                String line;
                while ((line = br.readLine()) != null) {
                    if (line.startsWith("#") || line.startsWith("Reported")) {
                        continue;
                    }
                    String[] tokens = line.split("\t");
                    if (tokens.length >= 8 && tokens[7] != null) {
                        String[] toks = tokens[0].split("_");
                        Long experimentId = Long.parseLong(toks[0]);
                        UUID rid = UUID.fromString(toks[1]);
                        Long readCount = Long.parseLong(toks[2]);
                        assert rid.version() == 1;

                        Boolean rc = true;
                        if (tokens[1].equals("+")) {
                            rc = false;
                        }

                        UUID id = UUIDs.timeBased();
//                    UUID id = UUIDGenerator.generate();
                        assert id.version() == 1;

                        toks = tokens[2].split("_");
                        Long tid = Long.parseLong(toks[1]);
                        Long startCoordinate = Long.parseLong(tokens[3]);
                        Double score = ((Integer) tokens[4].length()).doubleValue();
                        Long stopCoordinate = startCoordinate + score.longValue();
                        Integer mmc1 = 0, mmc2 = 0;
                        String v1 = "", v2 = "";
                        if (tokens[7].contains(",")) {
                            String[] tv1 = tokens[7].trim().split(",");

                            String[] tv = tv1[0].trim().split(":");
                            mmc1 = Integer.parseInt(tv[0]);
                            String[] tv2 = tv[1].split(">");
                            v1 = tv2[1];
                            tv = tv1[1].trim().split(":");
                            mmc2 = Integer.parseInt(tv[0]);
                            tv2 = tv[1].split(">");
                            v2 = tv2[1];

                            ta2_writer.addRow(experimentId, tid, rc, "S", "S", id, mmc1, mmc2, readCount, rid, score, startCoordinate, stopCoordinate, v1, v2);
                            tat2_writer.addRow(experimentId, rid, tid);
                        }
                    }
                    if (count % 100000 == 0) {
                        System.out.println("Uploaded " + count + " alignments in file: " + file);
                    }
                    count++;
                }

                br.close();
                ta2_writer.close();
                tat2_writer.close();
            } catch (IOException | NumberFormatException | InvalidRequestException e) {
                Logger.getLogger(CollateSAM.class.getName()).log(Level.SEVERE, null, e);
            }
            return file;
        }
    }

    static class CallableTranscriptUnmappedReads implements Callable {

        String file;
        String directory;

        public CallableTranscriptUnmappedReads(String file, String directory) {
            this.file = file;
            this.directory = directory;
        }

        @Override
        public Object call() throws Exception {
            try {
                System.out.println("Invoked callable..: " + file);
                CQLSSTableWriter.Builder builder = CQLSSTableWriter.builder();
                builder.inDirectory(directory + "/cache/unmappedreads")
                        .forTable(UMR_SCHEMA)
                        .using(UMR_INSERT_STMT)
                        .withPartitioner(new Murmur3Partitioner());
                CQLSSTableWriter umr_writer = builder.build();

                BufferedReader br
                        = new BufferedReader(new FileReader(new File(directory + "/" + file + ".un")));

                long count = 0;
                String line;
                while ((line = br.readLine()) != null) {
                    if (line.startsWith(">")) {
                        String[] tokens = line.split(">");
                        String[] toks = tokens[1].split("_");
                        Long experimentId = Long.parseLong(toks[0]);
                        UUID rid = UUID.fromString(toks[1]);
                        umr_writer.addRow(experimentId, rid);
                        if (count % 100000 == 0) {
                            System.out.println("Uploaded " + count + " unmappedreads in file: " + file);
                        }
                        count++;
                    }
                }

                br.close();
                umr_writer.close();
            } catch (Exception e) {
                Logger.getLogger(CollateSAM.class.getName()).log(Level.SEVERE, null, e);
            }
            return file;
        }
    }

    static class CallableReads implements Callable {

        String file;
        String directory;

        public CallableReads(String file, String directory) {
            this.file = file;
            this.directory = directory;
        }

        @Override
        public Object call() throws Exception {
            try {
                System.out.println("Invoked callable..: " + file);
                CQLSSTableWriter.Builder builder = CQLSSTableWriter.builder();
                builder.inDirectory(directory + "/cache/reads")
                        .forTable(READS_SCHEMA)
                        .using(READS_INSERT_STMT)
                        .withPartitioner(new Murmur3Partitioner());
                CQLSSTableWriter umr_writer = builder.build();

                BufferedReader br
                        = new BufferedReader(new FileReader(new File(directory + "/" + file)));

                String line;
                long c = 0;
                while ((line = br.readLine()) != null) {
                    String[] tokens = line.split("\t");
                    Long experimentId = Long.parseLong(tokens[0]);
                    UUID id = UUIDs.timeBased();
                    String seq = tokens[1];
                    Long count = Long.parseLong(tokens[2]);
                    umr_writer.addRow(experimentId, id, count, seq);
                    if (c % 100000 == 0) {
                        System.out.println("Uploaded " + c + " reads in file: " + file);
                    }
                    c++;
                }
                br.close();
                umr_writer.close();
            } catch (IOException | NumberFormatException | InvalidRequestException e) {
                Logger.getLogger(CollateSAM.class.getName()).log(Level.SEVERE, null, e);
            }
            return file;
        }
    }

    static class CallableTranscriptAlignmentsTss implements Callable {

        String file;
        String directory;

        public CallableTranscriptAlignmentsTss(String file, String directory) {
            this.file = file;
            this.directory = directory;
        }

        @Override
        public Object call() throws Exception {
            try {
                System.out.println("Invoked callable..: " + file);
                CQLSSTableWriter.Builder builder = CQLSSTableWriter.builder();
                builder.inDirectory(directory + "/cache/transcriptalignmenttss")
                        .forTable(TATSS_SCHEMA)
                        .using(TATSS_INSERT_STMT)
                        .withPartitioner(new Murmur3Partitioner());
                CQLSSTableWriter ta_writer = builder.build();

                builder = CQLSSTableWriter.builder();
                builder.inDirectory(directory + "/cache/transcriptalignmenttsstranspose")
                        .forTable(TATSST_SCHEMA)
                        .using(TATSST_INSERT_STMT)
                        .withPartitioner(new Murmur3Partitioner());
                CQLSSTableWriter tat_writer = builder.build();

                BufferedReader br
                        = new BufferedReader(new FileReader(new File(directory + "/" + file)));

                long count = 0;
                String line;
                while ((line = br.readLine()) != null) {
                    if (!(line.startsWith("#") || (line.startsWith("Reported")))) {
                        String[] tokens = line.split("\\s+");
                        String[] toks = tokens[0].split("_");
                        Integer key = Integer.parseInt(toks[0]);
                        UUID rid = UUID.fromString(toks[1]);
                        Long _c = Long.parseLong(toks[2]);
                        Boolean rc = true;
                        if (tokens[1].equals("+")) {
                            rc = false;
                        }

                        UUID id = UUIDs.timeBased();
                        toks = tokens[2].split("_");
                        Long tid = Long.parseLong(toks[1]);
                        Long startCoordinate = Long.parseLong(tokens[3]);
                        Double score = ((Integer) tokens[4].length()).doubleValue();
                        Long stopCoordinate = startCoordinate + score.longValue();
                        ta_writer.addRow(key, tid, rc, _c, id, rid, score, startCoordinate, stopCoordinate);
                        tat_writer.addRow(1l, rid, tid);
                        if (count % 100000 == 0) {
                            System.out.println("Uploaded " + count + " alignments in file: " + file);
                        }
                        count++;
                    }
                }
                br.close();
                ta_writer.close();
                tat_writer.close();
            } catch (Exception e) {
                Logger.getLogger(CollateSAM.class.getName()).log(Level.SEVERE, null, e);
            }
            return file;
        }
    }

}
