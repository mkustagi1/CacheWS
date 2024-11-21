package cache.dataimportes;

import cache.dataimportes.model.Transcript;
import cache.dataimportes.util.ConsumerService;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.gt;
import static com.datastax.driver.core.querybuilder.QueryBuilder.gte;
import static com.datastax.driver.core.querybuilder.QueryBuilder.lte;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.LogManager;
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
public class ElasticSearchTranscriptIndexer {

    final static Cluster CLUSTER = Cluster.builder()
            .withClusterName("cache")
            .addContactPoint("127.0.0.1")
            .build();

    final static Session SESSION = CLUSTER.connect();
    final static MappingManager MANAGER = new MappingManager(SESSION);
    final Mapper<Transcript> TRANSCRIPTMAPPER = MANAGER.mapper(Transcript.class);

    static final BlockingQueue<Callable> CONSUMER_QUEUE = new LinkedBlockingQueue<>();

    static {
        LogManager.getLogManager().reset();
        Logger globalLogger = Logger.getLogger(java.util.logging.Logger.GLOBAL_LOGGER_NAME);
        globalLogger.setLevel(java.util.logging.Level.WARNING);
        Thread consumerService = new Thread(new ConsumerService(CONSUMER_QUEUE));
        consumerService.setPriority(Thread.MAX_PRIORITY);
        consumerService.start();
    }

    public static void main(String[] args) {
        try {

            Callable callable = (Callable) () -> {
                try {

                    String index = "101";
                    System.out.println("Creating client");
                    Settings settings = Settings.builder()
                            .put("cluster.name", "cache")
                            .put("client.transport.sniff", true).build();
                    TransportClient client = new PreBuiltTransportClient(settings)
                            .addTransportAddress(new TransportAddress(InetAddress.getByName("cache01"), 9300));

                    Session session = CLUSTER.connect();
                    Statement stmt = QueryBuilder
                            .select()
                            .countAll()
                            .from("cache", "transcript")
                            .where(eq("key", 1));
                    ResultSet rs = session.execute(stmt);
                    Row row = rs.one();
                    Long rowCount = row.getLong(0);

                    System.out.println("rowCount: " + rowCount);
                    Integer numThreads = 25;
                    Long batchSize = (long) Math.ceil(rowCount.doubleValue() / numThreads.doubleValue());
                    System.out.println("batchSize: " + batchSize);
                    stmt = QueryBuilder
                            .select()
                            .column("transcriptid")
                            .from("cache", "transcript")
                            .where(eq("key", 1))
                            .orderBy(QueryBuilder.asc("transcriptid"))
                            .setFetchSize(100);
                    rs = session.execute(stmt);
                    int i = 0, tCount = 0;
                    Map<Integer, Long[]> bounds = new HashMap<>();
                    Long min = null, max = null;
                    while (!rs.isExhausted()) {
                        row = rs.one();
                        if (i == 0) {
                            min = row.getLong("transcriptid");
                        } else if (i == (tCount + 1) * batchSize) {
                            max = row.getLong("transcriptid");
                            bounds.put(tCount, new Long[]{min, max});
                            tCount++;
                            min = max;
                        } else if ((i == rowCount - 1) && (bounds.get(numThreads - 1) == null)) {
                            max = row.getLong("transcriptid");
                            bounds.put((numThreads - 1), new Long[]{min, max});
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
                        Long[] range = bounds.get(j);
                        System.out.println(j + ", min: " + range[0].toString() + ", max: " + range[1].toString());
                        CallableImplTranscripts t = new CallableImplTranscripts(j, range[0], range[1], index, session, client);
                        callables.add(t);
                    }
                    System.out.println("Invoking callables");
                    ex.invokeAll(callables);
                    ex.shutdown();

                    session.close();
                    client.close();
                } catch (Exception ex1) {
                    Logger.getLogger(ElasticSearchIndexer.class.getName()).log(Level.SEVERE, null, ex1);
                }
                return 1l;
            };
            CONSUMER_QUEUE.put(callable);

            callable = (Callable) () -> {
                shutdown();
                return 1d;
            };
            CONSUMER_QUEUE.put(callable);
        } catch (Exception ex) {
            Logger.getLogger(ElasticSearchTranscriptIndexer.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public static void shutdown() {
        SESSION.close();
        CLUSTER.close();
    }

    static class CallableImplTranscripts implements Callable {

        Long min;
        Long max;
        Integer maxReadLength;
        int threadId;
        String index;
        Session session;
        TransportClient client;

        public CallableImplTranscripts(int threadId, Long min, Long max, String index, Session session, TransportClient client) {
            this.threadId = threadId;
            this.min = min;
            this.max = max;
            this.index = "index_transcripts_" + index;
            this.maxReadLength = Integer.parseInt(index);
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
                        .from("cache", "transcript")
                        .where(eq("key", 1))
                        .and(gt("transcriptid", min))
                        .and(lte("transcriptid", max))
                        .setFetchSize(100);
                if (threadId == 0) {
                    stmt = QueryBuilder
                            .select()
                            .all()
                            .from("cache", "transcript")
                            .where(eq("key", 1))
                            .and(gte("transcriptid", min))
                            .and(lte("transcriptid", max))
                            .setFetchSize(100);
                }
                ResultSet rs = session.execute(stmt);
                int i = 0;
                BulkRequestBuilder bulkRequest = client.prepareBulk();
                while (!rs.isExhausted()) {
                    Row row = rs.one();
                    String sequence = row.getString("sequence");
                    Long tid = row.getLong("transcriptid");
                    String name = row.getString("name");
                    String json = "{"
                            + "\"transcriptid\":\"" + tid.toString() + "\","
                            + "\"name\":\"" + name + "\","
                            + "\"sequence\":\"" + sequence + "\""
                            + "}";

                    IndexRequestBuilder irb = client.prepareIndex(index, "record")
                            .setSource(json);
                    irb.setRouting("1");
                    bulkRequest.add(irb);

                    if (i % 10 == 0) {
                        BulkResponse resp = bulkRequest.execute().actionGet();
                        bulkRequest = client.prepareBulk();
                    }

                    if (i % 100 == 0) {
                        System.out.println("ElasticSearchTranscriptIndexer: Indexed " + i + " documents on thread: " + threadId);
                    }
                    i++;
                }
                BulkResponse resp = bulkRequest.execute().actionGet();
                long end = System.nanoTime();
                long seconds = TimeUnit.NANOSECONDS.toSeconds(end - start);
                System.out.println("All transcripts on thread " + threadId + " are indexed, it took " + seconds + " seconds");
            } catch (Exception e) {
                Logger.getLogger(ElasticSearchIndexer.class.getName()).log(Level.SEVERE, null, e);
            }
            return threadId;
        }
    }
}
