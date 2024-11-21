package cache.dataimportes;

import cache.dataimportes.model.Datastatistics;
import cache.dataimportes.util.ConsumerService;
import static cache.dataimportes.util.DataAccess.CLUSTER;
import static cache.dataimportes.util.DataAccess.DATASTATISTICSMAPPER;
import static cache.dataimportes.util.DataAccess.DATASTATISTICSQUERY;
import static cache.dataimportes.util.DataAccess.SESSION;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.gt;
import static com.datastax.driver.core.querybuilder.QueryBuilder.gte;
import static com.datastax.driver.core.querybuilder.QueryBuilder.lte;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

/**
 *
 * @author Manjunath Kustagi
 */
public class ElasticSearchIndexer {

    static final BlockingQueue<Callable> CONSUMER_QUEUE = new LinkedBlockingQueue<>();

    static {
//        LogManager.getLogManager().reset();
//        Logger globalLogger = Logger.getLogger(java.util.logging.Logger.GLOBAL_LOGGER_NAME);
//        globalLogger.setLevel(java.util.logging.Level.WARNING);
        Thread consumerService = new Thread(new ConsumerService(CONSUMER_QUEUE));
        consumerService.setPriority(Thread.MAX_PRIORITY);
        consumerService.start();
    }

    public static void main(String[] args) {
        try {

            BufferedReader br = new BufferedReader(new FileReader(new File(args[0])));
            Map<Long, Integer> indexMap = new HashMap<>();
            String line = "";
            System.out.println(args[0]);
            while ((line = br.readLine()) != null) {
                String[] tokens = line.split("\t");
                System.out.println("tokens: " + tokens[0] + ", " + tokens[1]);
                indexMap.put(Long.parseLong(tokens[0]), Integer.parseInt(tokens[1]));
            }

            for (final Long key : indexMap.keySet()) {

                final String index;

                switch (indexMap.get(key)) {
                    case 98:
                        index = "101";
                        break;
                    case 60:
                        index = "60";
                        break;
                    case 50:
                        index = "50";
                        break;
                    case 75:
                        index = "75";
                        break;
                    case 65:
                        index = "65";
                        break;
                    case 20:
                        index = "20";
                        break;
                    case 97:
                        index = "97";
                        break;
                    default:
                        index = "101";
                        break;
                }

                Callable callable = (Callable) () -> {
                    long experimentId = key;
                    try {
                        System.out.println("Creating client");
                        Settings settings = Settings.builder()
                                .put("cluster.name", "cache")
                                .put("client.transport.sniff", true).build();
                        TransportClient client = new PreBuiltTransportClient(settings)
                                .addTransportAddress(new TransportAddress(InetAddress.getByName("127.0.0.1"), 9300));

                        Session session = CLUSTER.connect();
                        BoundStatement bs = DATASTATISTICSQUERY.bind().setInt("ky", 1).setLong("eid", experimentId).setInt("d", 0);
                        ResultSet _rs = SESSION.execute(bs);
                        Datastatistics ds = DATASTATISTICSMAPPER.map(_rs).one();
                        Long rowCount = ds.getTotalreadsnonredundant();

                        System.out.println("rowCount: " + rowCount);
                        Integer numThreads = 25;
                        Long batchSize = (long) Math.ceil(rowCount.doubleValue() / numThreads.doubleValue());
                        System.out.println("batchSize: " + batchSize);
                        Statement stmt = QueryBuilder
                                .select()
                                .column("id")
                                .from("cache", "reads")
                                .where(eq("experimentid", experimentId))
                                .orderBy(QueryBuilder.asc("id"))
                                .setFetchSize(60000);
                        ResultSet rs = session.execute(stmt);
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
                            CallableImplReads t = new CallableImplReads(experimentId, j, range[0], range[1], index, session, client);
                            callables.add(t);
                        }
                        System.out.println("Invoking callables");
                        ex.invokeAll(callables);
                        ex.shutdown();

                        if (!(index.equals("20"))) {
                            ex = Executors.newFixedThreadPool(numThreads, (Runnable r) -> {
                                Thread t = new Thread(r);
                                t.setPriority(Thread.MAX_PRIORITY);
                                return t;
                            });
                            for (int j = 0; j < numThreads; j++) {
                                UUID[] range = bounds.get(j);
                                System.out.println(j + ", min: " + range[0].toString() + ", max: " + range[1].toString());
                                CallableImplReads40 t = new CallableImplReads40(experimentId, j, range[0], range[1], index, session, client);
                                callables.add(t);
                            }
                            System.out.println("Invoking callables 40");
                            ex.invokeAll(callables);
                            ex.shutdown();
                        }
                        session.close();
                        client.close();
                    } catch (UnknownHostException | InterruptedException ex1) {
                        Logger.getLogger(ElasticSearchIndexer.class.getName()).log(Level.SEVERE, null, ex1);
                    }
                    return experimentId;
                };
                CONSUMER_QUEUE.put(callable);
            }
            Callable callable = (Callable) () -> {
                shutdown();
                return 1d;
            };
            CONSUMER_QUEUE.put(callable);
        } catch (NumberFormatException | IOException | InterruptedException ex) {
            Logger.getLogger(ElasticSearchIndexer.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public static void shutdown() {
//        SESSION.close();
//        CLUSTER.close();
    }

    static class CallableImplReads implements Callable {

        Long experimentId;
        UUID min;
        UUID max;
        Integer maxReadLength;
        int threadId;
        String index;
        Session session;
        TransportClient client;

        public CallableImplReads(long experimentId, int threadId, UUID min, UUID max, String index, Session session, TransportClient client) {
            this.experimentId = experimentId;
            this.threadId = threadId;
            this.min = min;
            this.max = max;
            this.index = "index_reads_" + index;
            this.maxReadLength = Integer.parseInt(index);
            this.session = session;
            this.client = client;
        }

        @Override
        public Object call() {
            long start = System.nanoTime();
            Statement stmt = QueryBuilder
                    .select()
                    .all()
                    .from("cache", "reads")
                    .where(eq("experimentid", experimentId))
                    .and(gt("id", min))
                    .and(lte("id", max))
                    .setFetchSize(100000);
            if (threadId == 0) {
                stmt = QueryBuilder
                        .select()
                        .all()
                        .from("cache", "reads")
                        .where(eq("experimentid", experimentId))
                        .and(gte("id", min))
                        .and(lte("id", max))
                        .setFetchSize(100000);
            }
            ResultSet rs = session.execute(stmt);
            int i = 0;
            BulkRequestBuilder bulkRequest = client.prepareBulk();
            while (!rs.isExhausted()) {
                Row row = rs.one();
                String sequence = row.getString("sequence");
                String id = row.getUUID("id").toString();

                Map<String, Object> json = new HashMap<>();
                json.put("id", id);
                json.put("rid", experimentId);
                json.put("sequence", sequence);

                IndexRequestBuilder irb = client.prepareIndex(index, "record")
                        .setSource(json)
                        .setRouting(experimentId.toString());
                bulkRequest.add(irb);

                if (i % 100000 == 0) {
                    BulkResponse resp = bulkRequest.execute().actionGet();
                    bulkRequest = client.prepareBulk();
                }

                if (i % 100000 == 0) {
                    System.out.println("ElasticSearchIndexer: Indexed " + i + " documents from experiment " + experimentId + " on thread: " + threadId);
                }
                i++;
            }
            if (bulkRequest.numberOfActions() > 0) {
                BulkResponse resp = bulkRequest.execute().actionGet();
                bulkRequest = client.prepareBulk();
            }

            long end = System.nanoTime();
            long seconds = TimeUnit.NANOSECONDS.toSeconds(end - start);
            System.out.println("All reads in experiment " + experimentId + " on thread " + threadId + " are indexed, it took " + seconds + " seconds");
            return experimentId;
        }
    }

    static class CallableImplReads40 implements Callable {

        Long experimentId;
        UUID min;
        UUID max;
        int threadId;
        int maxLength;
        String index;
        Session session;
        TransportClient client;

        public CallableImplReads40(long experimentId, int threadId, UUID min, UUID max, String index, Session session, TransportClient client) {
            this.experimentId = experimentId;
            this.threadId = threadId;
            this.min = min;
            this.max = max;
            this.maxLength = Integer.parseInt(index);
            this.index = "index_reads_40";
            this.session = session;
            this.client = client;
        }

        @Override
        public Object call() {
            try {
                long start = System.nanoTime();
                Statement stmt = QueryBuilder
                        .select()
                        .all()
                        .from("cache", "reads")
                        .where(eq("experimentid", experimentId))
                        .and(gt("id", min))
                        .and(lte("id", max))
                        .setFetchSize(100000);
                if (threadId == 0) {
                    stmt = QueryBuilder
                            .select()
                            .all()
                            .from("cache", "reads")
                            .where(eq("experimentid", experimentId))
                            .and(gte("id", min))
                            .and(lte("id", max))
                            .setFetchSize(100000);
                }
                ResultSet rs = session.execute(stmt);
                int i = 0;

                BulkRequestBuilder bulkRequest = client.prepareBulk();
//                bulkRequest.setRefreshPolicy(RefreshPolicy.IMMEDIATE);
                while (!rs.isExhausted()) {
                    Row row = rs.one();
                    String id = row.getUUID("id").toString();
                    String sequence = row.getString("sequence");
                    
//                if (sequence.length() < maxLength) {
                    Map<String, Object> json = new HashMap<>();
                    json.put("id", id);
                    json.put("rid", experimentId);
                    json.put("sequence", sequence);

                    IndexRequestBuilder irb = client.prepareIndex(index, "record")
                            .setSource(json)
                            .setRouting(experimentId.toString());
                    bulkRequest.add(irb);
//                }

                    if (bulkRequest.numberOfActions() > 100000) {
                        BulkResponse resp = bulkRequest.execute().actionGet();
                        if (resp.hasFailures()) {
                            System.out.println("Has failures, row: " + i + ", thread: " + threadId);
                        }
                        bulkRequest = client.prepareBulk();
                    }
                    if (i % 100000 == 0) {
                        System.out.println("ElasticSearchIndexer: Indexed " + i + " documents from experiment " + experimentId + " on thread: " + threadId);
                    }
                    i++;
                }
                if (bulkRequest.numberOfActions() > 0) {
                    BulkResponse resp = bulkRequest.execute().actionGet();
                    bulkRequest = client.prepareBulk();
                }
                long end = System.nanoTime();
                long seconds = TimeUnit.NANOSECONDS.toSeconds(end - start);
                System.out.println("All reads in experiment " + experimentId + " on thread " + threadId + " are indexed, it took " + seconds + " seconds");
            } catch (Exception e) {
                Logger.getLogger(ElasticSearchIndexer.class.getName()).log(Level.SEVERE, null, e);
            }
            return experimentId;
        }
    }

}
