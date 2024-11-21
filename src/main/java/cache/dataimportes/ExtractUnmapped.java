package cache.dataimportes;

import cache.dataimportes.model.Reads;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Manjunath Kustagi
 */
public class ExtractUnmapped {

    final static Cluster CLUSTER = Cluster.builder()
            .withClusterName("cache")
            .addContactPoint("127.0.0.1")
            .build();

    final static Session SESSION = CLUSTER.connect();
//    final static MappingManager MANAGER = new MappingManager(SESSION);
//    final static Mapper<Reads> READSMAPPER = MANAGER.mapper(Reads.class);
//    final static PreparedStatement PREPARED = SESSION.prepare(
//            "select sequence from cache.reads where experimentid=? and id=?");

    static {
//        LogManager.getLogManager().reset();
//        Logger globalLogger = Logger.getLogger(java.util.logging.Logger.GLOBAL_LOGGER_NAME);
//        globalLogger.setLevel(java.util.logging.Level.WARNING);
    }

    public static void main(String[] args) {
        try {
//            Long[] eids = new Long[]{102, 103, 104, 105, 106, 107, 108, 109, 
//                10, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 11, 120, 121, 
//                122, 123, 124, 125, 126, 127, 128, 129, 12, 132, 133, 134, 135, 13, 
//                149, 14, 150, 151, 152, 153, 155, 156, 157, 158, 15, 161, 162, 163, 
//                164, 165, 166, 167, 168, 169, 16, 170, 171, 172, 173, 174, 175, 176, 
//                177, 178, 179, 17, 180, 183, 184, 185, 18, 191, 192, 193, 194, 195, 
//                19, 1, 206, 207, 20, 211, 212, 213, 214, 21, 220, 221, 222, 223, 224, 
//                225, 226, 227, 228, 229, 22, 240, 241, 242, 243, 244, 245, 246, 247, 
//                248, 249, 25, 266, 267, 268, 269, 274, 275, 276, 277, 279, 280, 281, 
//                282, 287, 289, 28, 291, 292, 293, 294, 295, 296, 297, 298, 299, 29, 
//                300, 301, 302, 303, 304, 305, 306, 307, 30, 31, 32, 33, 34, 35, 36, 
//                37, 38, 39, 3, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 55, 56, 57, 
//                59, 5, 63, 64, 65, 66, 67, 69, 6, 70, 71, 72, 73, 74, 75, 76, 77, 
//                78, 79, 7, 80, 9, 4, 278};

            Long[] eids = new Long[]{308l, 309l, 310l, 314l, 315l, 316l, 317l};

            ExecutorService ex = Executors.newFixedThreadPool(7, (Runnable r) -> {
                Thread t = new Thread(r);
                t.setPriority(Thread.MAX_PRIORITY);
                return t;
            });

            List<Callable<Object>> callables = new ArrayList<>();
            final String directory = args[0];
            for (Long id : eids) {
                CallableTranscriptUnmappedReads t = new CallableTranscriptUnmappedReads(id, directory);
                callables.add(t);
            }

            ex.invokeAll(callables);
            ex.shutdown();
            SESSION.close();
            CLUSTER.close();
        } catch (InterruptedException ex) {
            Logger.getLogger(CollateSAM.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    static class CallableTranscriptUnmappedReads implements Callable {

        Long id;
        String directory;

        public CallableTranscriptUnmappedReads(Long id, String directory) {
            this.id = id;
            this.directory = directory;
        }

        @Override
        public Object call() throws Exception {
            try {
                System.out.println("Invoked callable..: " + id);

                BufferedWriter bw = new BufferedWriter(new FileWriter(new File(directory + "/reads_" + id + ".fa")));

//                Statement stmt = QueryBuilder
//                        .select()
//                        .column("readid")
//                        .from("cache", "unmappedreads")
//                        .where(eq("experimentid", id))
//                        .setFetchSize(60000)
//                        .setReadTimeoutMillis(Integer.MAX_VALUE);
                Statement stmt = QueryBuilder
                        .select()
                        .column("id")
                        .column("sequence")
                        .column("count")
                        .from("cache", "reads")
                        .where(eq("experimentid", id))
                        .setFetchSize(60000)
                        .setReadTimeoutMillis(Integer.MAX_VALUE);

                ResultSet rs = SESSION.execute(stmt);

                long count = 0;
                long start = System.nanoTime();
                while (!rs.isExhausted()) {
                    Row row = rs.one();
                    UUID rid = row.getUUID("id");
                    String _s = row.getString("sequence");
                    Long _count = row.getLong("count");
//                    BoundStatement bound = PREPARED.bind()
//                            .setLong("experimentid", id)
//                            .setUUID("id", rid);
//                    ResultSet rs1 = SESSION.execute(bound);
//                    Row row1 = rs1.one();
//                    String _s = row1.getString("sequence");
//                    Reads reads = READSMAPPER.get(id.longValue(), rid);
//                    String _s = reads.getSequence();
                    bw.write(">" + id.toString() + "_" + rid.toString() + "_" + _count.toString());
                    bw.newLine();
                    bw.write(_s);
                    bw.newLine();
                    if (count % 200000 == 0) {
                        long end1 = System.nanoTime();
                        long seconds1 = TimeUnit.NANOSECONDS.toSeconds(end1 - start);
                        System.out.println("Extracted: " + count + " reads from experiment "
                                + id + " in " + seconds1 + " seconds");
                    }
                    count++;
                }
                bw.flush();
                bw.close();
            } catch (Exception e) {
                Logger.getLogger(CollateSAM.class.getName()).log(Level.SEVERE, null, e);
            }
            return id;
        }
    }

}
