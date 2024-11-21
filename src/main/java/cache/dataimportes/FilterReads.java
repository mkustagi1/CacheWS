package cache.dataimportes;

import cache.dataimportes.model.Datastatistics;
import cache.dataimportes.model.Reads;
import cache.dataimportes.model.Transcriptalignment;
import cache.dataimportes.model.Transcriptalignment1mismatch;
import cache.dataimportes.model.Transcriptalignment1mismatchtranspose;
import cache.dataimportes.model.Transcriptalignment2mismatch;
import cache.dataimportes.model.Transcriptalignment2mismatchtranspose;
import cache.dataimportes.model.Transcriptalignmenttranspose;
import cache.dataimportes.model.Unmappedreads;
import cache.dataimportes.util.ConsumerService;
import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.gt;
import static com.datastax.driver.core.querybuilder.QueryBuilder.gte;
import static com.datastax.driver.core.querybuilder.QueryBuilder.lte;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.LogManager;

public class FilterReads {

    final static Cluster CLUSTER = Cluster.builder()
            .withClusterName("cache")
            .addContactPoint("127.0.0.1")
            .build();

    final static Session SESSION = CLUSTER.connect();
    final MappingManager MANAGER = new MappingManager(SESSION);
    final Mapper<Reads> READSMAPPER = MANAGER.mapper(Reads.class);
    final Mapper<Unmappedreads> UNMAPPEDREADSMAPPER = MANAGER.mapper(Unmappedreads.class);
    final Mapper<Transcriptalignment> TRANSCRIPTALIGNMENTMAPPER = MANAGER.mapper(Transcriptalignment.class);
    final Mapper<Transcriptalignment1mismatch> TRANSCRIPTALIGNMENT1MISMATCHMAPPER = MANAGER.mapper(Transcriptalignment1mismatch.class);
    final Mapper<Transcriptalignment2mismatch> TRANSCRIPTALIGNMENT2MISMATCHMAPPER = MANAGER.mapper(Transcriptalignment2mismatch.class);
    final Mapper<Transcriptalignmenttranspose> TRANSCRIPTALIGNMENTTRANSPOSEMAPPER = MANAGER.mapper(Transcriptalignmenttranspose.class);
    final Mapper<Transcriptalignment1mismatchtranspose> TRANSCRIPTALIGNMENT1MISMATCHTRANSPOSEMAPPER = MANAGER.mapper(Transcriptalignment1mismatchtranspose.class);
    final Mapper<Transcriptalignment2mismatchtranspose> TRANSCRIPTALIGNMENT2MISMATCHTRANSPOSEMAPPER = MANAGER.mapper(Transcriptalignment2mismatchtranspose.class);
    final Mapper<Datastatistics> DATASTATISTICSMAPPER = MANAGER.mapper(Datastatistics.class);

    static PreparedStatement selectTa = SESSION.prepare("select reversecomplement, id, readid from cache.transcriptalignment "
            + "where experimentid=:ei and transcriptid=:ti");
    static PreparedStatement selectTa1 = SESSION.prepare("select reversecomplement, mismatchtype, id, readid from cache.transcriptalignment1mismatch "
            + "where experimentid=:ei and transcriptid=:ti");
    static PreparedStatement selectTa2 = SESSION.prepare("select reversecomplement, mismatch1type, mismatch2type, "
            + "id, readid from cache.transcriptalignment2mismatch "
            + "where experimentid=:ei and transcriptid=:ti");
    static PreparedStatement selectTat = SESSION.prepare("select transcriptid from cache.transcriptalignmenttranspose "
            + "where experimentid=:ei and readid=:ri");
    static PreparedStatement selectTat1 = SESSION.prepare("select transcriptid from cache.transcriptalignment1mismatchtranspose "
            + "where experimentid=:ei and readid=:ri");
    static PreparedStatement selectTat2 = SESSION.prepare("select transcriptid from cache.transcriptalignment2mismatchtranspose "
            + "where experimentid=:ei and readid=:ri");
    
    static final BlockingQueue<Callable> CONSUMER_QUEUE = new LinkedBlockingQueue<>();

    static {
        LogManager.getLogManager().reset();
        Logger globalLogger = Logger.getLogger(java.util.logging.Logger.GLOBAL_LOGGER_NAME);
        globalLogger.setLevel(java.util.logging.Level.WARNING);
        System.setProperty("org.apache.lucene.maxClauseCount", Integer.toString(Integer.MAX_VALUE));
        Thread consumerService = new Thread(new ConsumerService(CONSUMER_QUEUE));
        consumerService.setPriority(Thread.MAX_PRIORITY);
        consumerService.start();
    }

    enum TYPE {
        TRANSCRIPTS,
        READS
    }

    void init(List<Integer> indices, TYPE t) {
    }

    public void runAlignmentExactMatch(Long experimentId, Integer index) {
        try {
            Long rowCount = DATASTATISTICSMAPPER.get(experimentId).getTotalreadsnonredundant();
            System.out.println("rowCount: " + rowCount);

            Integer numThreads = 100;
            Long batchSize = (long) Math.ceil(rowCount.doubleValue() / numThreads.doubleValue());
            System.out.println("batchSize: " + batchSize);

            Statement stmt = QueryBuilder
                    .select()
                    .column("id")
                    .from("cache", "reads")
                    .where(eq("experimentid", experimentId))
                    .orderBy(QueryBuilder.asc("id"))
                    .setFetchSize(60000);
            ResultSet rs = SESSION.execute(stmt);

            int i = 0, tCount = 0;
            Map<Integer, UUID[]> bounds = new HashMap<>();
            UUID min = null, max = null;
            while (!rs.isExhausted()) {
                Row row = rs.one();

                if (i == 0) {
                    min = row.getUUID("id");
                } else if (i == (tCount + 1) * batchSize) {
                    max = row.getUUID("id");
                    bounds.put(tCount, new UUID[]{min, max});
                    tCount++;
                    min = max;
                } else if ((i == rowCount - 1) && (bounds.get(numThreads - 1) == null)) {
                    max = row.getUUID("id");
                    bounds.put((numThreads - 1), new UUID[]{min, max});
                }

                i++;
            }

            ExecutorService ex = Executors.newFixedThreadPool(numThreads, (Runnable r) -> {
                Thread t = new Thread(r);
                t.setPriority(Thread.MAX_PRIORITY);
                return t;
            });
            System.out.println("ExecutorService created..");

            List<Callable<Object>> callables = new ArrayList<>();
            for (int j = 0; j < numThreads; j++) {
                UUID[] range = bounds.get(j);
                System.out.println(j + ", min: " + range[0].toString() + ", max: " + range[1].toString());
                CallableImplReads t = new CallableImplReads(experimentId, j, range[0], range[1], index);
                callables.add(t);
            }

            System.out.println("Invoking callables");
            ex.invokeAll(callables);
            ex.shutdown();

            stmt = QueryBuilder
                    .select()
                    .column("count")
                    .from("cache", "reads")
                    .where(eq("experimentid", experimentId))
                    .orderBy(QueryBuilder.asc("id"))
                    .setFetchSize(60000);
            rs = SESSION.execute(stmt);
            
            Long totalReads = 0l;
            Long nrReads = 0l;
            
            while (!rs.isExhausted()) {
                Row row = rs.one();
                totalReads += row.getLong("count");
                nrReads++;
            }
            Datastatistics ds = DATASTATISTICSMAPPER.get(experimentId);
            ds.setTotalmappedreads(totalReads);
            ds.setTotalreadsnonredundant(nrReads);
            DATASTATISTICSMAPPER.save(ds);            
        } catch (NumberFormatException | InterruptedException e) {
            Logger.getLogger(FilterReads.class.getName()).log(Level.SEVERE, null, e);
        }
    }

    public static void shutdown() {
        SESSION.close();
        CLUSTER.close();
    }

    class CallableImplReads implements Callable {

        Long experimentId;
        UUID min;
        UUID max;
        Integer index;
        int threadId;
        Session session = CLUSTER.connect();

        public CallableImplReads(long experimentId, int threadId, UUID min, UUID max, Integer index) {
            try {
                this.experimentId = experimentId;
                this.threadId = threadId;
                this.min = min;
                this.max = max;
                this.index = index;
            } catch (Exception ex) {
                Logger.getLogger(FilterReads.class.getName()).log(Level.SEVERE, null, ex);
            }
        }

        @Override
        public Object call() {
            System.out.println("called threadId: " + threadId);
            final Object lock = new Object();

            long start = System.nanoTime();

            Statement stmt = QueryBuilder
                    .select()
                    .column("id")
                    .column("sequence")
                    .from("cache", "reads")
                    .where(eq("experimentid", experimentId))
                    .and(gt("id", min))
                    .and(lte("id", max))
                    .setFetchSize(100000);

            if (threadId == 0) {
                stmt = QueryBuilder
                        .select()
                        .column("id")
                        .column("sequence")
                        .from("cache", "reads")
                        .where(eq("experimentid", experimentId))
                        .and(gte("id", min))
                        .and(lte("id", max))
                        .setFetchSize(100000);
            }

            System.out.println("line 519..");
            ResultSet rs = session.execute(stmt);
            System.out.println("line 521..");

            int count = 0;
            BatchStatement batch = new BatchStatement();
            System.out.println("line 523..");
            while (!rs.isExhausted()) {
                Row row = rs.one();
                UUID rid = row.getUUID("id");
                String _s = row.getString("sequence");
                if (_s.length() < 40) {
                    batch.add(READSMAPPER.deleteQuery(experimentId, rid));
                    batch.add(UNMAPPEDREADSMAPPER.deleteQuery(experimentId, rid));

                    BoundStatement bs = selectTat.bind().setLong("ei", experimentId).setUUID("ri", rid);
                    ResultSet _rs = session.execute(bs);
                    while (!_rs.isExhausted()) {
                        Row _row = _rs.one();
                        long tid = _row.getLong("transcriptid");
                        BoundStatement bs1 = selectTa.bind().setLong("ei", experimentId).setLong("ti", tid);
                        ResultSet rs1 = session.execute(bs1);
                        while (!rs1.isExhausted()) {
                            Row row1 = rs1.one();
                            UUID rid1 = row1.getUUID("readid");
                            if (rid.equals(rid1)) {
                                batch.add(TRANSCRIPTALIGNMENTMAPPER.deleteQuery(experimentId, tid,
                                        row1.getBool("reversecomplment"),
                                        row1.getUUID("id")));
                            }
                        }
                        batch.add(TRANSCRIPTALIGNMENTTRANSPOSEMAPPER.deleteQuery(experimentId, rid, tid));
                    }

                    bs = selectTat1.bind().setLong("ei", experimentId).setUUID("ri", rid);
                    _rs = session.execute(bs);
                    while (!_rs.isExhausted()) {
                        Row _row = _rs.one();
                        long tid = _row.getLong("transcriptid");
                        BoundStatement bs1 = selectTa1.bind().setLong("ei", experimentId).setLong("ti", tid);
                        ResultSet rs1 = session.execute(bs1);
                        while (!rs1.isExhausted()) {
                            Row row1 = rs1.one();
                            UUID rid1 = row1.getUUID("readid");
                            if (rid.equals(rid1)) {
                                batch.add(TRANSCRIPTALIGNMENT1MISMATCHMAPPER.deleteQuery(experimentId, tid,
                                        row1.getBool("reversecomplment"),
                                        row1.getString("mismatchtype"),
                                        row1.getUUID("id")));
                            }
                        }
                        batch.add(TRANSCRIPTALIGNMENT1MISMATCHTRANSPOSEMAPPER.deleteQuery(experimentId, rid, tid));
                    }

                    bs = selectTat2.bind().setLong("ei", experimentId).setUUID("ri", rid);
                    _rs = session.execute(bs);
                    while (!_rs.isExhausted()) {
                        Row _row = _rs.one();
                        long tid = _row.getLong("transcriptid");
                        BoundStatement bs1 = selectTa2.bind().setLong("ei", experimentId).setLong("ti", tid);
                        ResultSet rs1 = session.execute(bs1);
                        while (!rs1.isExhausted()) {
                            Row row1 = rs1.one();
                            UUID rid1 = row1.getUUID("readid");
                            if (rid.equals(rid1)) {
                                batch.add(TRANSCRIPTALIGNMENT2MISMATCHMAPPER.deleteQuery(experimentId, tid,
                                        row1.getBool("reversecomplment"),
                                        row1.getString("mismatch1type"),
                                        row1.getString("mismatch2type"),
                                        row1.getUUID("id")));
                            }
                        }
                        batch.add(TRANSCRIPTALIGNMENT2MISMATCHTRANSPOSEMAPPER.deleteQuery(experimentId, rid, tid));
                    }
                }
                if (batch.size() > 1000) {
//                    session.execute(batch);
//                    batch.clear();
                }
                if (count % 100000 == 0) {
                    long end1 = System.nanoTime();
                    long seconds1 = TimeUnit.NANOSECONDS.toSeconds(end1 - start);
                    System.out.println("FilterReads: Checked " + count + " reads from experiment "
                            + experimentId + " on thread: " + threadId + " in " + seconds1 + " seconds");
                }
                count++;
            }
            if (batch.size() > 0) {
//                session.execute(batch);
//                batch.clear();
            }
            long end = System.nanoTime();
            long seconds = TimeUnit.NANOSECONDS.toSeconds(end - start);
            System.out.println("All reads in experiment " + experimentId + " on thread " + threadId + " are checked, it took " + seconds + " seconds");

            session.close();
            return threadId;
        }
    }

    public static void main(String[] args) {
        try {
            BufferedReader br = new BufferedReader(new FileReader(new File(args[0])));
            Map<Long, Integer> indexMap = new HashMap<>();
            String line = "";
            System.out.println(args[0]);
            System.out.println(FilterReads.class.getName());
            while ((line = br.readLine()) != null) {
                String[] tokens = line.split("\t");
                System.out.println("tokens: " + tokens[0] + ", " + tokens[1]);
                indexMap.put(Long.parseLong(tokens[0]), Integer.parseInt(tokens[1]));
            }
            br.close();
//            for (final Long key : indexMap.keySet()) {
//                final String index;
//                switch (indexMap.get(key)) {
//                    case 98:
//                        index = "101";
//                        break;
//                    case 50:
//                        index = "50";
//                        break;
//                    case 75:
//                        index = "75";
//                        break;
//                    case 65:
//                        index = "65";
//                        break;
//                    case 20:
//                        index = "20";
//                        break;
//                    case 97:
//                        index = "97";
//                        break;
//                    default:
//                        index = "101";
//                        break;
//                }
//
//                if (!index.equals("20")) {
            Callable callable = (Callable) () -> {
                FilterReads sstp0 = new FilterReads();
                sstp0.runAlignmentExactMatch(302l, 40);
                return "All alignments in datasets " + 302l + " completed.";
            };
            CONSUMER_QUEUE.put(callable);
//                }
//            }

            callable = (Callable) () -> {
                shutdown();
                return 1d;
            };
            CONSUMER_QUEUE.put(callable);
        } catch (InterruptedException ex) {
            Logger.getLogger(FilterReads.class.getName()).log(Level.SEVERE, null, ex);
        } catch (FileNotFoundException ex) {
            Logger.getLogger(FilterReads.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(FilterReads.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
