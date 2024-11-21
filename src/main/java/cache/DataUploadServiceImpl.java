package cache;

import cache.dataimportes.formbeans.DataUploadBean;
import cache.dataimportes.holders.ExperimentResult;
import cache.dataimportes.model.Experiment;
import cache.dataimportes.model.Experimentkey;
import static cache.dataimportes.util.DataAccess.EXPERIMENTCOUNTQUERY;
import static cache.dataimportes.util.DataAccess.EXPERIMENTKEYMAPPER;
import static cache.dataimportes.util.DataAccess.EXPERIMENTMAPPER;
import static cache.dataimportes.util.DataAccess.EXPERIMENTQUERY;
import static cache.dataimportes.util.DataAccess.SESSION;
import cache.interfaces.DataUploadService;
import com.caucho.hessian.server.HessianServlet;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.servlet.ServletConfig;

/**
 *
 * @author Manjunath Kustagi
 */
public class DataUploadServiceImpl extends HessianServlet implements DataUploadService {

    static Map<Integer, List<ExperimentResult>> PAGES = new HashMap<>();
    static int PAGESIZE = 100;

    @Override
    public long createExperiment(int paramId, DataUploadBean bean) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public long updateExperiment(int paramId, DataUploadBean bean) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public long uploadSequence(long expID, long count, String sequence) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void uploadExperiment(Long id, String filename) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void init(ServletConfig config) {
        try {
            super.init(config);
            initPages();
        } catch (Exception ex) {
            Logger.getLogger(DataUploadServiceImpl.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @Override
    public ExperimentResult getExperiment(int rowId, int page) {
        List<ExperimentResult> experiments = PAGES.get(page);
        ExperimentResult result = null;
        if (rowId < experiments.size()) {
            result = experiments.get(rowId);
        }
        return result;
    }

    @Override
    public long getExperimentCount() {
        BoundStatement bs = EXPERIMENTCOUNTQUERY.bind();
        ResultSet _rs = SESSION.execute(bs);
        Long count = _rs.one().getLong(0);
        return count;
    }

    @Override
    public int getExperimentStatus(long expId) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public boolean start() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public boolean stop() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public boolean pause() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public double progress() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    private static void initPages() {
        List<ExperimentResult> experiments = new ArrayList<>(PAGESIZE);
        int page = 0, count = 0;
        for (int key = 0; key < 40; key++) {
            Statement stmt1 = QueryBuilder
                    .select()
                    .all()
                    .from("anvesana", "experiment")
                    .where(eq("key", key))
                    .orderBy(QueryBuilder.asc("id"))
                    .limit(PAGESIZE)
                    .allowFiltering()
                    .enableTracing();

            ResultSet rs = SESSION.execute(stmt1);

            for (Row row : rs) {
                ExperimentResult result = new ExperimentResult();
                result.key = row.getInt("key");
                result.experimentID = row.getLong("id");
                result.distance = row.getInt("distance");
                result.cellLine = row.getString("library");
                result.investigator = row.getString("investigator");
                Map<String, String> attributes = row.getMap("attributes", String.class, String.class);
                String description = attributes.get("description");
                if (description != null) {
                    result.protein = description;
                } else {
                    result.protein = row.getString("library");
                }
                result.library = row.getString("library");
                result.method = row.getString("method");
                result.mappingParameters = row.getString("readlength");
                result.annotation = row.getString("annotation");
                experiments.add(result);
                count++;
                if ((count % PAGESIZE) == 0) {
                    PAGES.put(page++, experiments);
                    experiments = new ArrayList<>(PAGESIZE);
                }
            }
        }
        if (experiments.size() > 0) {
            PAGES.put(page, experiments);
        }
    }

    @Override
    public int getPageSize() {
        return PAGESIZE;
    }

    @Override
    public ExperimentResult getExperimentById(Long eid) {
        ExperimentResult result = null;
        try {
            Experimentkey key = EXPERIMENTKEYMAPPER.get(eid, 0);
            if (key != null) {
                result = new ExperimentResult();
                result.key = key.getKey();
                result.experimentID = key.getExperimentid();
                result.distance = key.getDistance();
                BoundStatement bs = EXPERIMENTQUERY.bind().setInt("ky", key.getKey()).setLong("id", key.getExperimentid()).setInt("d", key.getDistance());
                ResultSet _rs = SESSION.execute(bs);
                Experiment experiment = EXPERIMENTMAPPER.map(_rs).one();
                result.annotation = experiment.getAnnotation();
                result.library = experiment.getLibrary();
                result.mappingParameters = experiment.getReadlength();
                result.investigator = experiment.getAnnotation();
                result.method = experiment.getMethod();
                result.cellLine = experiment.getAttributes().get("cellline");
                String description = experiment.getAttributes().get("description");
                if (description != null) {
                    result.protein = description;
                } else {
                    result.protein = experiment.getLibrary();
                }
            }
        } catch (Exception e) {
            Logger.getLogger(DataUploadServiceImpl.class
                    .getName()).log(Level.SEVERE, null, e);
        }
        return result;
    }
}
