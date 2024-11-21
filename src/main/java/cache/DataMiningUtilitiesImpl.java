package cache;

import com.caucho.hessian.server.HessianServlet;
import cache.interfaces.DataMiningUtilities;

/**
 *
 * @author Manjunath Kustagi
 */
public class DataMiningUtilitiesImpl extends HessianServlet implements DataMiningUtilities {

    @Override
    public void collateReferenceTranscriptsAcrossDatasets() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void collateExpressionDistributionAcrossDatasets() {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
