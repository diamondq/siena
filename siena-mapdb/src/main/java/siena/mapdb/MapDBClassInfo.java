package siena.mapdb;

import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.mapdb.DB;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;

import siena.Generator;
import siena.Id;
import siena.core.base.ExtClassInfo;

public class MapDBClassInfo
    extends ExtClassInfo
{
    private static ConcurrentMap<Class<?>, MapDBClassInfo> sInfoMap =
        new ConcurrentHashMap<Class<?>, MapDBClassInfo>();
    private static HashSet<String> sExistingMaps = new HashSet<String>();
    public static DB sDB;
    public static MapDBSerializer sSerializer;

    public Generator mIDGenerator;
    public HTreeMap<Object, Object> mPrimaryMap;
    public HTreeMap<Object, Object> mSecondaryData;

    private static HTreeMap<Object, Object> createOrGetMap(String pName, Serializer<?> pSerializer)
    {
        if (sExistingMaps.contains(pName))
            return sDB.getHashMap(pName);
        else
        {
            sExistingMaps.add(pName);
            return sDB.getHashMap(pName);
//            return sDB.createHashMap(pName, false, sSerializer, sSerializer);
        }
    }

    private MapDBClassInfo(Class<?> pClass)
    {
        super(pClass);

        mPrimaryMap = createOrGetMap(mSienaInfo.tableName, sSerializer);

        /* Verify some of the properties */

        /* Check the type of ID field */

        Field idField = mSienaInfo.getIdField();
        Id id = idField.getAnnotation(Id.class);
        mIDGenerator = id.value();
        if (mIDGenerator == Generator.SEQUENCE)
            throw new RuntimeException("Only NONE, UUID and AUTO_GENERATE are valid ID generators for MapDB"); //$NON-NLS-1$
        else if (mIDGenerator == Generator.AUTO_INCREMENT)
        {
            mSecondaryData = createOrGetMap(mSienaInfo.tableName + "__SEC", null); //$NON-NLS-1$
            if (mSecondaryData.containsKey(mSienaInfo.tableName + "__INC") == false) //$NON-NLS-1$
                mSecondaryData.put(idField.getName() + "__INC", 0L); //$NON-NLS-1$
        }

        /* Query the schema to see what fields have keys */

        //Map<String, Object> componentData = ArezzoSingleton.sINSTANCE.getComponentData("cassandra"); //$NON-NLS-1$
    }

    public static MapDBClassInfo getClassInfo(Class<?> pClass, boolean pCreateIfMissing)
    {
        MapDBClassInfo info = sInfoMap.get(pClass);
        if ((info == null) && (pCreateIfMissing == true))
        {
            MapDBClassInfo newInfo = new MapDBClassInfo(pClass);
            if ((info = sInfoMap.putIfAbsent(pClass, newInfo)) == null)
                info = newInfo;
        }
        return info;
    }

    public static void clearClassInfo()
    {
        sInfoMap.clear();
    }
}
