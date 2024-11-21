package cache.dataimportes.formbeans;

import com.caucho.hessian.io.AbstractSerializerFactory;
import com.caucho.hessian.io.SerializerFactory;
import java.util.List;

/**
 *
 * @author Manjunath Kustagi
 */
public class HessianSerializerFactory extends SerializerFactory {

    public void setSerializerFactories(List<AbstractSerializerFactory> factories) {
        for (AbstractSerializerFactory factory : factories) {
            addFactory(factory);
        }
    }
}
