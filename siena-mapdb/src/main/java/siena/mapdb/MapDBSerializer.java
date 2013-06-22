package siena.mapdb;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.mapdb.Serializer;
import org.mapdb.SerializerPojo;

public class MapDBSerializer
    implements Serializer<Object>, Serializable
{

    private static final long serialVersionUID = -4937187696904668248L;
    private transient SerializerPojo mSerializer;

    public MapDBSerializer()
    {
        mSerializer = new SerializerPojo(null);
    }

    @SuppressWarnings("rawtypes")
	@Override
    public void serialize(DataOutput pOut, Object pValue)
        throws IOException
    {
        MapDBClassInfo info = MapDBClassInfo.getClassInfo(pValue.getClass(), false);
        if (info == null)
        {
            pOut.writeBoolean(false);
            mSerializer.serialize(pOut, pValue);
        }
        else
        {
            pOut.writeBoolean(true);
            pOut.writeUTF(info.mSienaInfo.clazz.getName());
            Map<String, Object> fields = new HashMap<String, Object>();
            for (Field field : info.mSienaInfo.allFields)
            {
                try
                {
                    Object value = field.get(pValue);
                    if (value != null)
                    {
                        if (value instanceof Collection)
                            processCollection((Collection) value);
                        else if (value.getClass().isPrimitive() == false)
                        {
                            MapDBClassInfo info2 = MapDBClassInfo.getClassInfo(value.getClass(), false);
                            if (info2 != null)
                                value = convertObjToKey(value, info2);
                        }
                    }
                    fields.put(field.getName(), value);
                }
                catch (IllegalArgumentException ex)
                {
                    throw new RuntimeException(ex);
                }
                catch (IllegalAccessException ex)
                {
                    throw new RuntimeException(ex);
                }
            }
            mSerializer.serialize(pOut, fields);
        }
    }

    private Object convertObjToKey(Object pObj, MapDBClassInfo pInfo)
    {
        Map<String, Object> keys = new HashMap<String, Object>();
        for (Field f : pInfo.mSienaInfo.keys)
        {
            try
            {
                keys.put(f.getName(), f.get(pObj));
            }
            catch (IllegalArgumentException ex)
            {
                throw new RuntimeException(ex);
            }
            catch (IllegalAccessException ex)
            {
                throw new RuntimeException(ex);
            }
        }
        return keys;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private void processCollection(Collection pCollection)
    {
        Collection results = new ArrayList();
        for (Object o : pCollection)
        {
            MapDBClassInfo info = MapDBClassInfo.getClassInfo(o.getClass(), false);
            if (info == null)
                results.add(o);
            else
                results.add(convertObjToKey(o, info));
        }
        pCollection.clear();
        pCollection.addAll(results);
    }

    @Override
    public Object deserialize(DataInput pIn, int pAvailable)
        throws IOException
    {
        boolean isOurs = pIn.readBoolean();
        pAvailable -= 1;
        if (isOurs == false)
            return mSerializer.deserialize(pIn, pAvailable);
        else
        {
            String className = pIn.readUTF();
            pAvailable -= 2 + className.length();
            @SuppressWarnings("unchecked")
            Map<String, Object> fields = (Map<String, Object>) mSerializer.deserialize(pIn, pAvailable);
            try
            {
                Class<?> clazz = Class.forName(className);
                MapDBClassInfo info = MapDBClassInfo.getClassInfo(clazz, false);
                Object newObj = clazz.newInstance();
                for (Field field : info.mSienaInfo.allFields)
                {
                    Object value = fields.get(field.getName());
                    field.set(newObj, value);
                }
                return newObj;
            }
            catch (ClassNotFoundException ex)
            {
                throw new RuntimeException(ex);
            }
            catch (InstantiationException ex)
            {
                throw new RuntimeException(ex);
            }
            catch (IllegalAccessException ex)
            {
                throw new RuntimeException(ex);
            }
        }
    }

}
