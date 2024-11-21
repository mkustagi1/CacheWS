package cache.dataimportes.formbeans;

import com.caucho.hessian.io.AbstractSerializerFactory;
import com.caucho.hessian.io.Deserializer;
import com.caucho.hessian.io.HessianProtocolException;
import com.caucho.hessian.io.Serializer;

/**
 *
 * @author Manjunath Kustagi
 */
public class DataUploadBeanSerializerFactory extends AbstractSerializerFactory {

    @Override
    public Serializer getSerializer(Class cl) throws HessianProtocolException {
        if (DataUploadBean.class.isAssignableFrom(cl)) {
            return new DataUploadBeanSerializer();
        }
        return null;
    }

    @Override
    public Deserializer getDeserializer(Class cl) throws HessianProtocolException {
        if (DataUploadBean.class.isAssignableFrom(cl)) {
            return new DataUploadBeanDeSerializer();
        }
        return null;
    }
}
