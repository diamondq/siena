package siena.mapdb;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;

import org.mapdb.DB;

import siena.Query;
import siena.QueryOrder;
import siena.core.base.BaseOptions;
import siena.core.base.BasePersistenceManager;
import siena.core.base.MapToKeyTransformer;
import siena.core.base.MapToObjectTransformer;

import com.eaio.uuid.UUID;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

public class MapDBPersistenceManager
		extends
		BasePersistenceManager<MapDBClassInfo, MapDBQueryInfo, Map<String, Object>> {

	private final LoadingCache<MapDBClassInfo, Function<Map<String, Object>, ?>> sTransformerToObjectMap = CacheBuilder
			.newBuilder()
			.build(new CacheLoader<MapDBClassInfo, Function<Map<String, Object>, ?>>() {
				public com.google.common.base.Function<java.util.Map<String, Object>, ?> load(
						MapDBClassInfo pClass) throws Exception {
					return new MapToObjectTransformer<MapDBClassInfo, MapDBQueryInfo>(
							false, MapDBPersistenceManager.this, pClass);
				};
			});

	private final LoadingCache<MapDBClassInfo, Function<Map<String, Object>, ?>> sTransformerToObjectKeysMap = CacheBuilder
			.newBuilder()
			.build(new CacheLoader<MapDBClassInfo, Function<Map<String, Object>, ?>>() {
				public com.google.common.base.Function<java.util.Map<String, Object>, ?> load(
						MapDBClassInfo pClass) throws Exception {
					return new MapToObjectTransformer<MapDBClassInfo, MapDBQueryInfo>(
							true, MapDBPersistenceManager.this, pClass);
				};
			});

	private final LoadingCache<MapDBClassInfo, Function<Map<String, Object>, Object>> sTransformerToKeysMap = CacheBuilder
			.newBuilder()
			.build(new CacheLoader<MapDBClassInfo, Function<Map<String, Object>, Object>>() {
				public Function<Map<String, Object>, Object> load(
						MapDBClassInfo pClass) throws Exception {
					return new MapToKeyTransformer<MapDBClassInfo, MapDBQueryInfo>(
							MapDBPersistenceManager.this, pClass);
				};
			});

	DB mDB;
	private ConcurrentMap<Class<?>, MapDBClassInfo> mInfoMap = new ConcurrentHashMap<Class<?>, MapDBClassInfo>();

	public MapDBPersistenceManager() {
		super();
	}

	public MapDBPersistenceManager(DB pDB) {
		mDB = pDB;
	}

	/**
	 * @see siena.PersistenceManager#init(java.util.Properties)
	 */
	@Override
	public void init(Properties p) {
	}

	/**
	 * @see siena.PersistenceManager#insert(java.lang.Object)
	 */
	@Override
	public void insert(Object obj) {
		MapDBClassInfo ccInfo = getClassInfo(obj.getClass(), true);
		List<MapData> data = convertToMap(ccInfo, obj);

		for (MapData d : data) {
			if (d.type.equals("table")) //$NON-NLS-1$
			{
				if (d.name.equals(ccInfo.mSienaInfo.tableName)) {
					ccInfo.mPrimaryMap.put(collapseKeyToString(d.key), d.data);
				}
			}
		}

	}

	/**
	 * @see siena.core.base.BasePersistenceManager#deleteByKey(java.lang.Class,
	 *      java.lang.Object)
	 */
	@Override
	protected <T> void deleteByKey(Class<T> pClazz, Object pKey) {
		MapDBClassInfo clInfo = getClassInfo(pClazz, true);
		clInfo.mPrimaryMap.remove(collapseKeyToString(pKey));
	}

	public void exit() {
		mDB.commit();
		mDB.close();
	}

	/**
	 * @see siena.core.base.BasePersistenceManager#getClassInfo(java.lang.Class,
	 *      boolean)
	 */
	@Override
	protected MapDBClassInfo getClassInfo(Class<?> pClass,
			boolean pCreateIfMissing) {
		MapDBClassInfo info = mInfoMap.get(pClass);
		if ((info == null) && (pCreateIfMissing == true)) {
			MapDBClassInfo newInfo = new MapDBClassInfo(pClass, this);
			if ((info = mInfoMap.putIfAbsent(pClass, newInfo)) == null)
				info = newInfo;
		}
		return info;
	}

	/**
	 * @see siena.core.base.BasePersistenceManager#generateNextSequence(siena.mapdb.ExtClassInfo,
	 *      java.lang.String, java.lang.String)
	 */
	@Override
	protected long generateNextSequence(MapDBClassInfo pClassInfo,
			String pTableName, String pColumnName) {
		synchronized (this) {
			Long value = (Long) pClassInfo.mSecondaryData.get(pTableName
					+ "." + pColumnName + "__INC"); //$NON-NLS-1$//$NON-NLS-2$
			if (value == null)
				value = 0L;
			value = value + 1;
			pClassInfo.mSecondaryData.put(pTableName
					+ "." + pColumnName + "__INC", value); //$NON-NLS-1$ //$NON-NLS-2$
			return value;
		}
	}

	/**
	 * @see siena.core.base.BasePersistenceManager#constructQueryInfo()
	 */
	@Override
	protected MapDBQueryInfo constructQueryInfo() {
		return new MapDBQueryInfo();
	}

	protected <T, O> Iterator<O> retrieveData(MapDBClassInfo pCLInfo,
			Query<T> pQuery, MapDBQueryInfo pQueryInfo, int pLimit,
			Object pOffset, Function<Map<String, Object>, O> pTransformer) {

		BaseOptions opts = getStandardOptions(pQuery, pLimit, pOffset);

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
		for (Entry<Object, Object> pair : pCLInfo.mPrimaryMap.entrySet()) {
			@SuppressWarnings("unchecked")
			Map<String, Object> o = (Map<String, Object>) pair.getValue();
			if (checkAgainstQuery(o, pCLInfo, pQueryInfo) == true) {
				if ((hasOrder == false) && (opts.offset > skip)) {
					skip++;
					continue;
				}
				source.add(o);
				if ((hasOrder == false) && (opts.limit != -1)) {
					count++;
					if (count >= opts.limit)
						break;
				}
			}
		}

		Iterator<O> i = buildSortedIterator(source, pQuery, pCLInfo,
				pQueryInfo, pTransformer, (handledLimitAndOffset == true ? -1
						: opts.limit), (handledLimitAndOffset == true ? 0
						: opts.offset), opts.pageOpt);

		resetOpts(pQuery);

		return i;
	}


	@SuppressWarnings("unchecked")
	@Override
	protected <T> Function<Map<String, Object>, T> getTransformerToObject(
			MapDBClassInfo pCLInfo) {
		try {
			return (Function<Map<String, Object>, T>) sTransformerToObjectMap
					.get(pCLInfo);
		} catch (ExecutionException ex) {
			throw Throwables.propagate(ex);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	protected <T> Function<Map<String, Object>, T> getTransformerToObjectKeys(
			MapDBClassInfo pCLInfo) {
		try {
			return (Function<Map<String, Object>, T>) sTransformerToObjectKeysMap
					.get(pCLInfo);
		} catch (ExecutionException ex) {
			throw Throwables.propagate(ex);
		}
	}

	@Override
	protected Function<Map<String, Object>, Object> getTransformerToKeys(
			MapDBClassInfo pCLInfo) {
		try {
			return sTransformerToKeysMap.get(pCLInfo);
		} catch (ExecutionException ex) {
			throw Throwables.propagate(ex);
		}
	}

	/**
	 * @see siena.core.base.BasePersistenceManager#getFromRaw(java.lang.Object ,
	 *      siena.core.base.ExtClassInfo, java.lang.reflect.Field)
	 */
	@Override
	protected Object getFromRaw(Map<String, Object> pObj,
			MapDBClassInfo pCLInfo, Field pField) {
		String colName = pCLInfo.mFieldToColumnNameMap.get(pField);
		return pObj.get(colName);
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
			throw new RuntimeException(
					"Unsupported ID type " + pUUIDClass.getName()); //$NON-NLS-1$
	}
}
