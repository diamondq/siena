package siena.mapdb;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;

import org.mapdb.Atomic;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.TxMaker;

import siena.BaseQueryData;
import siena.QueryOrder;
import siena.SienaException;
import siena.core.async.SienaFuture;
import siena.core.base.BaseOptions;
import siena.core.base.BasePersistenceManager;
import siena.core.base.ConstantSienaFuture;
import siena.core.base.ExtTransScope;
import siena.core.base.ExtTransScope.CallData;
import siena.core.base.LowToHighTransformer;
import siena.core.base.MapToKeyTransformer;
import siena.core.base.TransResultType;
import siena.mapdb.MapDBPersistenceManager.TransScope;

import com.eaio.uuid.UUID;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Iterators;

public class MapDBPersistenceManager extends BasePersistenceManager<MapDBClassInfo, MapDBQueryInfo<?>, TransScope, Map<String, Object>> {

    private final LoadingCache<MapDBClassInfo, Function<Map<String, Object>, ?>>                   sTransformerToObjectMap     =
                                                                                                                                   CacheBuilder.newBuilder().build(new CacheLoader<MapDBClassInfo, Function<Map<String, Object>, ?>>() {
                                                                                                                                                                       public
                                                                                                                                                                           com.google.common.base.Function<java.util.Map<String, Object>, ?>
                                                                                                                                                                           load(
                                                                                                                                                                               MapDBClassInfo pClass)
                                                                                                                                                                               throws Exception {
                                                                                                                                                                           return new LowToHighTransformer<MapDBClassInfo, MapDBQueryInfo<?>, TransScope, Map<String, Object>>(
                                                                                                                                                                                                                                                                               false,
                                                                                                                                                                                                                                                                               MapDBPersistenceManager.this,
                                                                                                                                                                                                                                                                               pClass);
                                                                                                                                                                       };
                                                                                                                                                                   });

    private final LoadingCache<MapDBClassInfo, Function<Map<String, Object>, ?>>                   sTransformerToObjectKeysMap =
                                                                                                                                   CacheBuilder.newBuilder().build(new CacheLoader<MapDBClassInfo, Function<Map<String, Object>, ?>>() {
                                                                                                                                                                       public
                                                                                                                                                                           com.google.common.base.Function<java.util.Map<String, Object>, ?>
                                                                                                                                                                           load(
                                                                                                                                                                               MapDBClassInfo pClass)
                                                                                                                                                                               throws Exception {
                                                                                                                                                                           return new LowToHighTransformer<MapDBClassInfo, MapDBQueryInfo<?>, TransScope, Map<String, Object>>(
                                                                                                                                                                                                                                                                               true,
                                                                                                                                                                                                                                                                               MapDBPersistenceManager.this,
                                                                                                                                                                                                                                                                               pClass);
                                                                                                                                                                       };
                                                                                                                                                                   });

    private final LoadingCache<MapDBClassInfo, Function<Map<String, Object>, Map<String, Object>>> sTransformerToKeysMap       =
                                                                                                                                   CacheBuilder.newBuilder().build(new CacheLoader<MapDBClassInfo, Function<Map<String, Object>, Map<String, Object>>>() {
                                                                                                                                                                       public
                                                                                                                                                                           Function<Map<String, Object>, Map<String, Object>>
                                                                                                                                                                           load(
                                                                                                                                                                               MapDBClassInfo pClass)
                                                                                                                                                                               throws Exception {
                                                                                                                                                                           return new MapToKeyTransformer<MapDBClassInfo, MapDBQueryInfo<?>, TransScope, Map<String, Object>>(
                                                                                                                                                                                                                                                                              MapDBPersistenceManager.this,
                                                                                                                                                                                                                                                                              pClass);
                                                                                                                                                                       };
                                                                                                                                                                   });

    private ConcurrentMap<Class<?>, MapDBClassInfo>                                                mInfoMap                    =
                                                                                                                                   new ConcurrentHashMap<Class<?>, MapDBClassInfo>();
    private DBMaker                                                                                mDBMaker;
    private TxMaker                                                                                mTxMaker;
    private DB                                                                                     mNonTransDB;
    private WeakHashMap<ThreadData, Boolean>                                                       mThreadDatas                =
                                                                                                                                   new WeakHashMap<ThreadData, Boolean>();
    private ThreadLocal<ThreadData>                                                                mThreadLocal                =
                                                                                                                                   new ThreadLocal<ThreadData>();
    private TransactionType                                                                        mTransType;

    public MapDBPersistenceManager() {
        super();
    }

    public enum TransactionType {
        AUTO, IMPLICIT, EXPLICIT
    };

    public MapDBPersistenceManager(DBMaker pDBMaker, TransactionType pType) {
        mDBMaker = pDBMaker;
        mTransType = pType;
    }

    /**
     * @see siena.PersistenceManager#init(java.util.Properties)
     */
    @Override
    public void init(Properties p) {
    }

    public static class ThreadData {
        public DB         db;
        public boolean    activeTransaction = false;
        public boolean    txDB              = false;
        public TransScope rootScope;
    }

    public static class TransScope extends ExtTransScope {

        public ThreadData tData;

    }

    @Override
    protected TransScope openTransactionScope(boolean pImplicitTransaction) {
        TransScope scope = new TransScope();
        scope.tData = mThreadLocal.get();
        if (scope.tData == null) {
            scope.tData = new ThreadData();
            mThreadLocal.set(scope.tData);
            mThreadDatas.put(scope.tData, Boolean.TRUE);
        }
        scope.keepOpen = (scope.tData.activeTransaction == false ? false : true);
        if (scope.keepOpen == false)
            scope.tData.rootScope = scope;
        if (pImplicitTransaction == true) {
            if (scope.tData.db == null) {
                if (mTransType == TransactionType.EXPLICIT) {
                    /*
                     * We've been asked to stay explicit, so open a fresh transaction database
                     */
                    if (mTxMaker == null)
                        mTxMaker = mDBMaker.makeTxMaker();
                    scope.tData.db = mTxMaker.makeTx();
                    scope.tData.txDB = true;
                } else if (mTransType == TransactionType.IMPLICIT) {
                    /*
                     * We've been asked to stay implicit, so just keep using the non-trans DB
                     */
                    if (mNonTransDB == null)
                        mNonTransDB = mDBMaker.make();
                    scope.tData.db = mNonTransDB;
                    scope.tData.txDB = false;
                } else if (mTransType == TransactionType.AUTO) {
                    /*
                     * It's set to auto, so, if we've previously switched to transactional, then stay there,
                     * otherwise use a non-transaction db
                     */
                    if (mTxMaker != null) {
                        scope.tData.db = mTxMaker.makeTx();
                        scope.tData.txDB = true;
                    } else {
                        if (mNonTransDB == null)
                            mNonTransDB = mDBMaker.make();
                        scope.tData.db = mNonTransDB;
                        scope.tData.txDB = false;
                    }
                }
            }
        } else {
            if (scope.tData.db != null) {
                /*
                 * There's already a database open, if it's a non-transaction database, then close it. If it's
                 * transactional, then it's an error, because we don't support nested transactions.
                 */
                if (scope.tData.txDB == true)
                    throw new SienaException("Nested transactions are not supported");
                else {
                    if ((mTransType == TransactionType.EXPLICIT) || (mTransType == TransactionType.AUTO)) {
                        /*
                         * We need to attempt to close the non-transaction database. We'll wait for up to N
                         * seconds, and prevent new ones from spawning
                         */
                        scope.tData.db.commit();
                        scope.tData.db.close();
                        scope.tData.db = null;
                        mNonTransDB = null;
                    }
                }
            }
            /* Open a new transaction database */
            if (scope.tData.db == null) {
                if ((mTransType == TransactionType.EXPLICIT) || (mTransType == TransactionType.AUTO)) {
                    if (mTxMaker == null)
                        mTxMaker = mDBMaker.makeTxMaker();
                    scope.tData.db = mTxMaker.makeTx();
                    scope.tData.txDB = true;
                } else if (mTransType == TransactionType.IMPLICIT) {
                    if (mNonTransDB == null)
                        mNonTransDB = mDBMaker.make();
                    scope.tData.db = mNonTransDB;
                    scope.tData.txDB = false;
                }
            }
        }
        scope.tData.activeTransaction = true;
        return scope;
    }

    @Override
    protected void closeTransactionScope(TransScope scope) {
        if (scope.keepOpen == false) {
            if (scope.tData.txDB == true) {
                if ((scope.transResult == TransResultType.COMMIT) || (scope.transResult == TransResultType.READONLY)) {
                    scope.tData.db.commit();
                    TransScope rootScope = getRootTransScope(scope);
                    if (rootScope.afterSuccess != null)
                        for (CallData<Object> cd : rootScope.afterSuccess) {
                            cd.function.apply(cd.data);
                        }
                } else
                    scope.tData.db.rollback();
                if (scope.tData.txDB == true)
                    scope.tData.db = null;
            } else {
                if ((scope.transResult == TransResultType.COMMIT) || (scope.transResult == TransResultType.READONLY)) {
                    TransScope rootScope = getRootTransScope(scope);
                    if (rootScope.afterSuccess != null)
                        for (CallData<Object> cd : rootScope.afterSuccess) {
                            cd.function.apply(cd.data);
                        }
                }
            }
            scope.tData.activeTransaction = false;
        }
    }

    /**
     * @see siena.core.base.BasePersistenceManager#getRootTransScope(siena.core.base.ExtTransScope)
     */
    @Override
    protected TransScope getRootTransScope(TransScope pTransScope) {
        if (pTransScope.tData.rootScope == null)
            return pTransScope;
        return pTransScope.tData.rootScope;
    }

    protected HTreeMap<Object, Object> getPrimaryMap(TransScope pScope, MapDBClassInfo pCLInfo) {
        HTreeMap<Object, Object> map = pScope.tData.db.getHashMap(pCLInfo.mSienaInfo.tableName);
        return map;
    }

    /**
     * @see siena.core.base.BasePersistenceManager#storeMapData(siena.core.base.ExtTransScope,
     *      siena.core.base.ExtClassInfo, java.util.List, boolean)
     */
    @Override
    protected void storeMapData(TransScope pScope, MapDBClassInfo pCLInfo, List<MapData<Map<String, Object>>> pData, boolean pIsInsert) {
        for (MapData<Map<String, Object>> d : pData) {
            if (d.type.equals("table")) //$NON-NLS-1$
            {
                if (d.name.equals(pCLInfo.mSienaInfo.tableName)) {
                    String key = collapseKeyToString(d.key, pCLInfo, d.name);
                    getPrimaryMap(pScope, pCLInfo).put(key, d.data);
                }
            }
        }
    }

    /**
     * @see siena.core.base.BasePersistenceManager#deleteByLowKey(siena.core.base.ExtClassInfo,
     *      java.lang.Object)
     */
    @Override
    protected SienaFuture<Boolean> deleteByLowKey(MapDBClassInfo pCLInfo, Map<String, Object> pLowKey) {
        TransScope scope = openTransactionScope(true);
        try {
            SienaFuture<Boolean> result = ConstantSienaFuture.sBOOLEAN_TRUE;
            String key = collapseKeyToString(pLowKey, pCLInfo, pCLInfo.mSienaInfo.tableName);
            if (getPrimaryMap(scope, pCLInfo).remove(key) == null)
                result = ConstantSienaFuture.sBOOLEAN_FALSE;
            scope.transResult = TransResultType.COMMIT;
            return result;
        } finally {
            closeTransactionScope(scope);
        }
    }

    /**
     * @see siena.core.base.BasePersistenceManager#loadLowFromLowKeys(siena.core.base.ExtClassInfo,
     *      java.lang.Object)
     */
    @SuppressWarnings("unchecked")
    @Override
    protected Map<String, Object> loadLowFromLowKeys(MapDBClassInfo pCLInfo, Map<String, Object> keys) {
        TransScope scope = openTransactionScope(true);
        try {
            String key = collapseKeyToString(keys, pCLInfo, pCLInfo.mSienaInfo.tableName);
            Map<String, Object> results = (Map<String, Object>) getPrimaryMap(scope, pCLInfo).get(key);
            scope.transResult = TransResultType.COMMIT;
            return results;
        } finally {
            closeTransactionScope(scope);
        }
    }

    public void exit() {
        if (mNonTransDB != null)
            mNonTransDB.close();
        if (mTxMaker != null)
            mTxMaker.close();
    }

    /**
     * @see siena.core.base.BasePersistenceManager#getClassInfo(java.lang.Class, boolean)
     */
    @Override
    protected MapDBClassInfo getClassInfo(Class<?> pClass, boolean pCreateIfMissing) {
        MapDBClassInfo info = mInfoMap.get(pClass);
        if ((info == null) && (pCreateIfMissing == true)) {
            MapDBClassInfo newInfo = new MapDBClassInfo(pClass, this);
            if ((info = mInfoMap.putIfAbsent(pClass, newInfo)) == null)
                info = newInfo;
        }
        return info;
    }

    /**
     * @see siena.core.base.BasePersistenceManager#generateNextSequence(siena.core.base.ExtClassInfo,
     *      java.lang.String)
     */
    @Override
    protected long generateNextSequence(MapDBClassInfo pClassInfo, String pColumnName) {
        TransScope scope = openTransactionScope(true);
        try {
            Atomic.Integer aInt = Atomic.getInteger(scope.tData.db, pClassInfo.mSienaInfo.tableName + "." + pColumnName + "__INC");
            int result = aInt.get();
            result = result + 1;
            aInt.set(result);
            scope.transResult = TransResultType.COMMIT;
            return result;
        } finally {
            closeTransactionScope(scope);
        }
    }

    /**
     * @see siena.core.base.BasePersistenceManager#createQueryInfo(siena.core.base.ExtClassInfo,
     *      siena.BaseQueryData, boolean)
     */
    @Override
    protected <T> MapDBQueryInfo<T> createQueryInfo(MapDBClassInfo pCLInfo, BaseQueryData<T> pQuery, boolean pKeysOnly) {
        return new MapDBQueryInfo<T>(pCLInfo, pQuery, pKeysOnly);
    }

    protected <T, O> Iterator<O> retrieveData(TransScope pScope, MapDBClassInfo pCLInfo, BaseQueryData<T> pQuery,
        MapDBQueryInfo<?> pQueryInfo, int pLimit, Object pOffset, Function<Map<String, Object>, O> pTransformer) {

        BaseOptions opts = getStandardOptions(pQuery, pLimit, pOffset);
        if ((opts.cursorOpt.cursorLimit == 0) || (opts.cursorOpt.isAtBeginning == true))
            return Iterators.emptyIterator();

        Collection<Map<String, Object>> source = new ArrayList<Map<String, Object>>();

        final List<QueryOrder> orderBys = pQuery.getOrders();
        boolean hasOrder = false;
        boolean handledLimitAndOffset = true;

        if ((orderBys != null) && (orderBys.size() > 0)) {
            hasOrder = true;
            handledLimitAndOffset = false;
        }

        /* Searching by index */

        /* Searching by key */

        /* Searching by tablescan */

        int count = 0;
        int skip = 0;
        for (Entry<Object, Object> pair : getPrimaryMap(pScope, pCLInfo).entrySet()) {
            @SuppressWarnings("unchecked")
            Map<String, Object> o = (Map<String, Object>) pair.getValue();
            if (checkAgainstQuery(o, pCLInfo, pQueryInfo) == true) {
                if ((hasOrder == false) && (opts.cursorOpt.cursorOffset > skip)) {
                    skip++;
                    continue;
                }
                source.add(o);
                if (hasOrder == false) {
                    count++;
                    if (count >= opts.cursorOpt.cursorLimit)
                        break;
                }
            }
        }

        Iterator<O> i = buildSortedIterator(source, pQuery, pCLInfo, pQueryInfo, pTransformer, handledLimitAndOffset, opts);

        return i;
    }

    /**
     * @see siena.core.base.BasePersistenceManager#getFunctionFromLowToHigh(siena.core.base.ExtClassInfo,
     *      boolean)
     */
    @SuppressWarnings("unchecked")
    @Override
    protected <T> Function<Map<String, Object>, T> getFunctionFromLowToHigh(MapDBClassInfo pCLInfo, boolean pOnlyKeys) {
        try {
            if (pOnlyKeys == true)
                return (Function<Map<String, Object>, T>) sTransformerToObjectKeysMap.get(pCLInfo);
            else
                return (Function<Map<String, Object>, T>) sTransformerToObjectMap.get(pCLInfo);
        } catch (ExecutionException ex) {
            throw Throwables.propagate(ex);
        }
    }

    /**
     * @see siena.core.base.BasePersistenceManager#getFunctionFromLowToLowKey(siena.core.base.ExtClassInfo)
     */
    @Override
    protected Function<Map<String, Object>, Map<String, Object>> getFunctionFromLowToLowKey(MapDBClassInfo pCLInfo) {
        try {
            return sTransformerToKeysMap.get(pCLInfo);
        } catch (ExecutionException ex) {
            throw Throwables.propagate(ex);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    protected <T> T generateUUID(Class<T> pUUIDClass) {
        if (pUUIDClass == String.class)
            return (T) new UUID().toString();
        else if (pUUIDClass == UUID.class)
            return (T) new UUID();
        else if (pUUIDClass == java.util.UUID.class) {
            UUID aUUID = new UUID();
            return (T) new java.util.UUID(aUUID.time, aUUID.clockSeqAndNode);
        } else
            throw new RuntimeException("Unsupported ID type " + pUUIDClass.getName()); //$NON-NLS-1$
    }

    /**
     * @see siena.core.base.BasePersistenceManager#save(java.lang.Object)
     */
    @Override
    public void save(Object obj) {
        insert(obj);
    }

    /**
     * @see siena.core.base.BasePersistenceManager#getLowColumnNames(java.lang.Object)
     */
    @Override
    protected Set<String> getLowColumnNames(Map<String, Object> pKey) {
        return pKey.keySet();
    }

    /**
     * @see siena.core.base.BasePersistenceManager#supportsGetSetTransactions()
     */
    @Override
    protected boolean supportsGetSetTransactions() {
        return true;
    }

    /**
     * @see siena.core.base.BasePersistenceManager#createLow(siena.core.base.ExtClassInfo)
     */
    @Override
    protected Map<String, Object> createLow(MapDBClassInfo pCLInfo) {
        return new HashMap<String, Object>();
    }

    /**
     * @see siena.core.base.BasePersistenceManager#getLowColumnValue(java.lang.Object,
     *      siena.core.base.ExtClassInfo, java.lang.Class, java.lang.reflect.Field)
     */
    @SuppressWarnings("unchecked")
    @Override
    protected <T> T getLowColumnValue(Map<String, Object> pObj, MapDBClassInfo pCLInfo, Class<T> pResultClass, Field pField) {
        String colName = pCLInfo.mFieldToColumnNameMap.get(pField);
        Object v = pObj.get(colName);
        if ((v == null) || ((v != null) && ((pResultClass == Object.class) || (pResultClass.isInstance(v) == false)))) {
            if ((v instanceof String) == false)
                v = convertToString(pCLInfo, pField, v.getClass(), v);
            return convertFromString(pCLInfo, pField, pResultClass, (String) v);
        }
        return (T) v;
    }

    /**
     * @see siena.core.base.BasePersistenceManager#getLowColumnValue(java.lang.Object,
     *      siena.core.base.ExtClassInfo, java.lang.Class, java.lang.String)
     */
    @SuppressWarnings("unchecked")
    @Override
    protected <T> T getLowColumnValue(Map<String, Object> pObj, MapDBClassInfo pCLInfo, Class<T> pResultClass, String pColumnName) {
        Object v = pObj.get(pColumnName);
        Field field = pCLInfo.mColumnNameToFieldMap.get(pColumnName);
        if ((v == null) || ((v != null) && ((pResultClass == Object.class) || (pResultClass.isInstance(v) == false)))) {
            if ((v instanceof String) == false)
                v = convertToString(pCLInfo, field, v.getClass(), v);
            return convertFromString(pCLInfo, field, pResultClass, (String) v);
        }
        return (T) v;
    }

    /**
     * @see siena.core.base.BasePersistenceManager#convertToRawLowColumnValue(siena.core.base.ExtClassInfo,
     *      java.lang.reflect.Field, java.lang.Object)
     */
    @Override
    protected Object convertToRawLowColumnValue(MapDBClassInfo pCLInfo, Field pField, Object pValue) {
        return convertToPrimitive(pCLInfo, pField, pField.getType(), pValue);
    }

    /**
     * @see siena.core.base.BasePersistenceManager#compareRawToLowColumnValue(java.lang.Object,
     *      siena.core.base.ExtClassInfo, java.lang.reflect.Field, java.lang.Object)
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    protected int compareRawToLowColumnValue(Map<String, Object> pObj, MapDBClassInfo pCLInfo, Field pField, Object pRawValue) {
        String colName = pCLInfo.mFieldToColumnNameMap.get(pField);
        Object ov = pObj.get(colName);
        Object tv = pRawValue;
        if ((ov == null) && (tv == null))
            return 0;
        if ((ov == null) && (tv != null))
            return 1;
        if ((ov != null) && (tv == null))
            return -1;
        if (ov instanceof Comparable)
            return ((Comparable) ov).compareTo(tv);
        else {
            if (ov.equals(tv) == true)
                return 0;
            else
                return -1;
        }
    }

    @Override
    protected void setLowColumnValue(Map<String, Object> pObj, MapDBClassInfo pCLInfo, Field pField, Object pConversionValue) {
        String colName = pCLInfo.mFieldToColumnNameMap.get(pField);
        pObj.put(colName, convertToPrimitive(pCLInfo, pField, pField.getType(), pConversionValue));
    }

    @Override
    protected String convertLowToString(Map<String, Object> pObj) {
        throw new IllegalArgumentException("Not yet implemented");
    }

    @Override
    protected Map<String, Object> convertStringToLow(String pValue) {
        throw new IllegalArgumentException("Not yet implemented");
    }
}
