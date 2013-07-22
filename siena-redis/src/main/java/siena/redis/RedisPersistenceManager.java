package siena.redis;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.EncoderException;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;
import siena.BaseQueryData;
import siena.QueryOrder;
import siena.core.async.SienaFuture;
import siena.core.base.BaseOptions;
import siena.core.base.BasePersistenceManager;
import siena.core.base.ConstantSienaFuture;
import siena.core.base.ExtTransScope;
import siena.core.base.ExtTransScope.CallData;
import siena.core.base.FunctionSienaFuture;
import siena.core.base.LowToHighTransformer;
import siena.core.base.MapToKeyTransformer;
import siena.core.base.TransResultType;
import siena.redis.RedisPersistenceManager.TransScope;

import com.eaio.uuid.UUID;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Iterators;

public class RedisPersistenceManager extends
	BasePersistenceManager<RedisClassInfo, RedisQueryInfo<?>, TransScope, Map<String, String>>
{

	private static final Function<Response<Long>, Boolean>											sConvertIfGreater			=
																																	new ConvertIfGreaterFunction();

	private final LoadingCache<RedisClassInfo, Function<Map<String, String>, ?>>					sTransformerToObjectMap		=
																																	CacheBuilder
																																		.newBuilder()
																																		.build(
																																			new CacheLoader<RedisClassInfo, Function<Map<String, String>, ?>>()
																																			{
																																				public com.google.common.base.Function<java.util.Map<String, String>, ?> load(
																																					RedisClassInfo pClass)
																																					throws Exception
																																				{
																																					return new LowToHighTransformer<RedisClassInfo, RedisQueryInfo<?>, TransScope, Map<String, String>>(
																																						false,
																																						RedisPersistenceManager.this,
																																						pClass);
																																				};
																																			});

	private final LoadingCache<RedisClassInfo, Function<Map<String, String>, ?>>					sTransformerToObjectKeysMap	=
																																	CacheBuilder
																																		.newBuilder()
																																		.build(
																																			new CacheLoader<RedisClassInfo, Function<Map<String, String>, ?>>()
																																			{
																																				public com.google.common.base.Function<java.util.Map<String, String>, ?> load(
																																					RedisClassInfo pClass)
																																					throws Exception
																																				{
																																					return new LowToHighTransformer<RedisClassInfo, RedisQueryInfo<?>, TransScope, Map<String, String>>(
																																						true,
																																						RedisPersistenceManager.this,
																																						pClass);
																																				};
																																			});

	private final LoadingCache<RedisClassInfo, Function<Map<String, String>, Map<String, String>>>	sTransformerToKeysMap		=
																																	CacheBuilder
																																		.newBuilder()
																																		.build(
																																			new CacheLoader<RedisClassInfo, Function<Map<String, String>, Map<String, String>>>()
																																			{
																																				public Function<Map<String, String>, Map<String, String>> load(
																																					RedisClassInfo pClass)
																																					throws Exception
																																				{
																																					return new MapToKeyTransformer<RedisClassInfo, RedisQueryInfo<?>, TransScope, Map<String, String>>(
																																						RedisPersistenceManager.this,
																																						pClass);
																																				};
																																			});

	private ConcurrentMap<Class<?>, RedisClassInfo>													mInfoMap					=
																																	new ConcurrentHashMap<Class<?>, RedisClassInfo>();
	private WeakHashMap<ThreadData, Boolean>														mThreadDatas				=
																																	new WeakHashMap<ThreadData, Boolean>();
	private ThreadLocal<ThreadData>																	mThreadLocal				=
																																	new ThreadLocal<ThreadData>();
	private TransactionType																			mTransType;

	private JedisPool																				mJedisPool;

	public RedisPersistenceManager()
	{
		super();
	}

	public enum TransactionType
	{
		AUTO, IMPLICIT, EXPLICIT
	};

	public RedisPersistenceManager(JedisPool pJedisPool, TransactionType pType)
	{
		mJedisPool = pJedisPool;
		mTransType = pType;
	}

	/**
	 * @see siena.PersistenceManager#init(java.util.Properties)
	 */
	@Override
	public void init(Properties p)
	{
		String transStr = p.getProperty("redis.transaction.type", "AUTO");
		mTransType = TransactionType.valueOf(transStr);
		String str;

		JedisPoolConfig poolConfig = new JedisPoolConfig();

		if ((str = p.getProperty("redis.maxActive", null)) != null)
			poolConfig.setMaxActive(Integer.parseInt(str));

		if ((str = p.getProperty("redis.maxIdle", null)) != null)
			poolConfig.setMaxIdle(Integer.parseInt(str));

		if ((str = p.getProperty("redis.maxWait", null)) != null)
			poolConfig.setMaxWait(Integer.parseInt(str));

		if ((str = p.getProperty("redis.minEvictableIdleTimeMillis", null)) != null)
			poolConfig.setMinEvictableIdleTimeMillis(Long.parseLong(str));

		if ((str = p.getProperty("redis.minIdle", null)) != null)
			poolConfig.setMinIdle(Integer.parseInt(str));

		if ((str = p.getProperty("redis.numTestsPerEvictionRun", null)) != null)
			poolConfig.setNumTestsPerEvictionRun(Integer.parseInt(str));

		if ((str = p.getProperty("redis.softMinEvictableIdleTimeMillis", null)) != null)
			poolConfig.setSoftMinEvictableIdleTimeMillis(Long.parseLong(str));

		if ((str = p.getProperty("redis.testOnBorrow", null)) != null)
			poolConfig.setTestOnBorrow(Boolean.parseBoolean(str));

		if ((str = p.getProperty("redis.testOnReturn", null)) != null)
			poolConfig.setTestOnReturn(Boolean.parseBoolean(str));

		if ((str = p.getProperty("redis.testWhileIdle", null)) != null)
			poolConfig.setTestWhileIdle(Boolean.parseBoolean(str));

		if ((str = p.getProperty("redis.timeBetweenEvictionRunsMillis", null)) != null)
			poolConfig.setTimeBetweenEvictionRunsMillis(Long.parseLong(str));

		if ((str = p.getProperty("redis.whenExhaustedAction", null)) != null)
			poolConfig.setWhenExhaustedAction(Byte.parseByte(str));

		String host = "localhost";
		int port = Protocol.DEFAULT_PORT;
		int timeout = Protocol.DEFAULT_TIMEOUT;
		String password = null;
		int database = Protocol.DEFAULT_DATABASE;

		if ((str = p.getProperty("redis.host", null)) != null)
			host = str;
		if ((str = p.getProperty("redis.port", null)) != null)
			port = Integer.parseInt(str);
		if ((str = p.getProperty("redis.timeout", null)) != null)
			timeout = Integer.parseInt(str);
		if ((str = p.getProperty("redis.password", null)) != null)
			password = str;
		if ((str = p.getProperty("redis.host", null)) != null)
			database = Integer.parseInt(str);

		mJedisPool = new JedisPool(poolConfig, host, port, timeout, password, database);
		
	}

	public static class ThreadData
	{
		public Jedis		db;
		public Transaction	trans;
		public boolean		activeTransaction	= false;
		public TransScope	rootScope;
	}

	public static class TransScope extends ExtTransScope
	{

		public ThreadData	tData;

	}

	/**
	 * @see siena.core.base.BasePersistenceManager#openTransactionScope(boolean)
	 */
	@Override
	protected TransScope openTransactionScope(boolean pImplicitTransaction)
	{
		TransScope scope = new TransScope();
		scope.tData = mThreadLocal.get();
		if (scope.tData == null)
		{
			scope.tData = new ThreadData();
			mThreadLocal.set(scope.tData);
			mThreadDatas.put(scope.tData, Boolean.TRUE);
		}
		scope.keepOpen = (scope.tData.activeTransaction == false ? false : true);
		if (scope.keepOpen == false)
			scope.tData.rootScope = scope;

		if (pImplicitTransaction == true)
		{
			if (scope.tData.db == null)
			{
				if (mTransType == TransactionType.EXPLICIT)
				{
					throw new IllegalArgumentException("Not implemented");
				}
				else if (mTransType == TransactionType.IMPLICIT)
				{
					scope.tData.db = mJedisPool.getResource();
				}
				else if (mTransType == TransactionType.AUTO)
				{
					scope.tData.db = mJedisPool.getResource();
				}
			}
		}
		else
		{
			if (scope.tData.db == null)
			{
				if (mTransType == TransactionType.EXPLICIT)
				{
					throw new IllegalArgumentException("Not implemented");
				}
				else if (mTransType == TransactionType.IMPLICIT)
				{
					scope.tData.db = mJedisPool.getResource();
				}
				else if (mTransType == TransactionType.AUTO)
				{
					scope.tData.db = mJedisPool.getResource();
				}
			}
			else
				throw new IllegalArgumentException("Nested transactions are not supported");
			scope.tData.trans = scope.tData.db.multi();
		}
		scope.tData.activeTransaction = true;
		return scope;
	}

	/**
	 * @see siena.core.base.BasePersistenceManager#closeTransactionScope(siena.core.base.ExtTransScope)
	 */
	@Override
	protected void closeTransactionScope(TransScope scope)
	{
		if (scope.keepOpen == false)
		{
			if (scope.tData.trans != null)
			{
				if ((scope.transResult == TransResultType.COMMIT) || (scope.transResult == TransResultType.READONLY))
				{
					scope.tData.trans.exec();
					TransScope rootScope = getRootTransScope(scope);
					if (rootScope.afterSuccess != null)
						for (CallData<Object> cd : rootScope.afterSuccess)
						{
							cd.function.apply(cd.data);
						}
				}
				else
					scope.tData.trans.discard();
				scope.tData.trans = null;
			}
			else
			{
				if ((scope.transResult == TransResultType.COMMIT) || (scope.transResult == TransResultType.READONLY))
				{
					TransScope rootScope = getRootTransScope(scope);
					if (rootScope.afterSuccess != null)
						for (CallData<Object> cd : rootScope.afterSuccess)
						{
							cd.function.apply(cd.data);
						}
				}
			}
			mJedisPool.returnResource(scope.tData.db);
			scope.tData.db = null;
			scope.tData.activeTransaction = false;
		}
	}

	/**
	 * @see siena.core.base.BasePersistenceManager#getRootTransScope(siena.core.base.ExtTransScope)
	 */
	@Override
	protected TransScope getRootTransScope(TransScope pTransScope)
	{
		if (pTransScope.tData.rootScope == null)
			return pTransScope;
		return pTransScope.tData.rootScope;
	}

	/**
	 * @see siena.core.base.BasePersistenceManager#storeMapData(siena.core.base.ExtTransScope,
	 *      siena.core.base.ExtClassInfo, java.util.List, boolean)
	 */
	@Override
	protected void storeMapData(TransScope pScope, RedisClassInfo pCLInfo, List<MapData<Map<String, String>>> pData,
		boolean pIsInsert)
	{
		for (MapData<Map<String, String>> d : pData)
		{
			if (d.type.equals("table")) //$NON-NLS-1$
			{
				if (d.name.equals(pCLInfo.mSienaInfo.tableName))
				{
					String key = collapseKeyToString(d.key, pCLInfo, d.name);
					if (pScope.tData.trans != null)
					{
						pScope.tData.trans.sadd(pCLInfo.mSienaInfo.tableName, key);
						pScope.tData.trans.hmset(key, d.data);
					}
					else
					{
						pScope.tData.db.sadd(pCLInfo.mSienaInfo.tableName, key);
						pScope.tData.db.hmset(key, d.data);
					}
				}
			}
		}
	}

	/**
	 * @see siena.core.base.BasePersistenceManager#deleteByLowKey(siena.core.base.ExtClassInfo, java.lang.Object)
	 */
	@Override
	protected SienaFuture<Boolean> deleteByLowKey(RedisClassInfo pCLInfo, Map<String, String> pLowKey)
	{
		TransScope scope = openTransactionScope(true);
		try
		{
			SienaFuture<Boolean> result;
			String key = collapseKeyToString(pLowKey, pCLInfo, pCLInfo.mSienaInfo.tableName);
			if (scope.tData.trans != null)
			{
				scope.tData.trans.srem(pCLInfo.mSienaInfo.tableName, key);
				Response<Long> changeCount = scope.tData.trans.del(key);
				result = new FunctionSienaFuture<Response<Long>, Boolean>(sConvertIfGreater, changeCount);
			}
			else
			{
				scope.tData.db.srem(pCLInfo.mSienaInfo.tableName, key);
				Long changeCount = scope.tData.db.del(key);
				if (changeCount > 0)
					result = ConstantSienaFuture.sBOOLEAN_TRUE;
				else
					result = ConstantSienaFuture.sBOOLEAN_FALSE;
			}
			scope.transResult = TransResultType.COMMIT;
			return result;
		}
		finally
		{
			closeTransactionScope(scope);
		}
	}

	/**
	 * @see siena.core.base.BasePersistenceManager#loadLowFromLowKeys(siena.core.base.ExtClassInfo, java.util.Map)
	 */
	@Override
	protected Map<String, String> loadLowFromLowKeys(RedisClassInfo clInfo, Map<String, String> keys)
	{
		TransScope transScope = openTransactionScope(true);
		try
		{
			if (transScope.tData.trans != null)
				throw new IllegalArgumentException("Not yet implemented");
			Map<String, String> results =
				transScope.tData.db.hgetAll(collapseKeyToString(keys, clInfo, clInfo.mSienaInfo.tableName));
			if (transScope.transResult == TransResultType.ROLLBACK)
				transScope.transResult = TransResultType.COMMIT;
			return (results.isEmpty() == true ? null : results);
		}
		finally
		{
			closeTransactionScope(transScope);
		}
	}

	public void exit()
	{
		mJedisPool.destroy();
	}

	/**
	 * @see siena.core.base.BasePersistenceManager#getClassInfo(java.lang.Class, boolean)
	 */
	@Override
	protected RedisClassInfo getClassInfo(Class<?> pClass, boolean pCreateIfMissing)
	{
		RedisClassInfo info = mInfoMap.get(pClass);
		if ((info == null) && (pCreateIfMissing == true))
		{
			RedisClassInfo newInfo = new RedisClassInfo(pClass, this);
			if ((info = mInfoMap.putIfAbsent(pClass, newInfo)) == null)
				info = newInfo;
		}
		return info;
	}

	/**
	 * @see siena.core.base.BasePersistenceManager#generateNextSequence(siena.core.base.ExtClassInfo, java.lang.String)
	 */
	@Override
	protected long generateNextSequence(RedisClassInfo pCLInfo, String pColumnName)
	{
		TransScope transScope = openTransactionScope(true);
		try
		{
			/*
			 * NOTE: Because get/set transactions are not supported, if we're in a transaction, then we need to generate
			 * a new connection to Redis to do the sequence number.
			 */
			long result;
			if (transScope.tData.trans != null)
			{
				Jedis newResource = mJedisPool.getResource();
				result = newResource.incr(pCLInfo.mSienaInfo.tableName + "." + pColumnName + "__INC");
				mJedisPool.returnResource(newResource);
			}
			else
				result = transScope.tData.db.incr(pCLInfo.mSienaInfo.tableName + "." + pColumnName + "__INC");
			if (transScope.transResult == TransResultType.ROLLBACK)
				transScope.transResult = TransResultType.COMMIT;
			return result;
		}
		finally
		{
			closeTransactionScope(transScope);
		}
	}

	@Override
	protected <T> RedisQueryInfo<T> createQueryInfo(RedisClassInfo pCLInfo, BaseQueryData<T> pQuery, boolean pKeysOnly)
	{
		return new RedisQueryInfo<T>(pCLInfo, pQuery, pKeysOnly);
	}

	/**
	 * @see siena.core.base.BasePersistenceManager#retrieveData(siena.core.base.ExtTransScope,
	 *      siena.core.base.ExtClassInfo, siena.Query, siena.core.base.BaseQueryInfo, int, java.lang.Object,
	 *      com.google.common.base.Function)
	 */
	@Override
	protected <T, O> Iterator<O> retrieveData(TransScope pScope, RedisClassInfo pCLInfo, BaseQueryData<T> pQuery,
		RedisQueryInfo<?> pQueryInfo, int pLimit, Object pOffset, Function<Map<String, String>, O> pTransformer)
	{
		if (pScope.tData.trans != null)
			throw new IllegalArgumentException("Not yet implemented");

		BaseOptions opts = getStandardOptions(pQuery, pLimit, pOffset);
		if ((opts.cursorOpt.cursorLimit == 0) || (opts.cursorOpt.isAtBeginning == true))
			return Iterators.emptyIterator();

		Collection<Map<String, String>> source = new ArrayList<Map<String, String>>();

		final List<QueryOrder> orderBys = pQuery.getOrders();
		boolean hasOrder = false;
		boolean handledLimitAndOffset = true;

		if ((orderBys != null) && (orderBys.size() > 0))
		{
			hasOrder = true;
			handledLimitAndOffset = false;
		}

		/* Searching by index */

		/* Searching by key */

		/* Searching by tablescan */

		int count = 0;
		int skip = 0;
		for (String key : pScope.tData.db.smembers(pCLInfo.mSienaInfo.tableName))
		{
			Map<String, String> o = pScope.tData.db.hgetAll(key);
			if (o.isEmpty() == true)
			{
				/*
				 * Since we always store at least the key, then an empty result indicates that the key no exists. This
				 * is an indicator that the related tables have gotten out of sync. Initiate a rebuild and restart
				 */
				rebuildRelations(pScope, pCLInfo.mSienaInfo.tableName);
				return retrieveData(pScope, pCLInfo, pQuery, pQueryInfo, pLimit, pOffset, pTransformer);
			}
			if (checkAgainstQuery(o, pCLInfo, pQueryInfo) == true)
			{
				if ((hasOrder == false) && (opts.cursorOpt.cursorOffset > skip))
				{
					skip++;
					continue;
				}
				source.add(o);
				if (hasOrder == false)
				{
					count++;
					if (count >= opts.cursorOpt.cursorLimit)
						break;
				}
			}
		}

		Iterator<O> i =
			buildSortedIterator(source, pQuery, pCLInfo, pQueryInfo, pTransformer, handledLimitAndOffset, opts);

		return i;
	}

	/**
	 * This internal method is used to rebuild all the relationships for a given table. It should only be called when
	 * there is a problem or that the structure has changed.
	 * 
	 * @param pScope a scope
	 * @param pTableName the table to rebuild
	 */
	private void rebuildRelations(TransScope pScope, String pTableName)
	{
		if (pScope.tData.trans != null)
			throw new IllegalArgumentException("Not yet implemented");

		/* Start by removing all the keys from the set */

		pScope.tData.db.del(pTableName);

		/* Now iterate through all keys in the hash set */

		for (String key : pScope.tData.db.keys(pTableName + ":*"))
		{
			pScope.tData.db.sadd(pTableName, key);
		}

		pScope.transResult = TransResultType.COMMIT;
	}

	/**
	 * @see siena.core.base.BasePersistenceManager#getFunctionFromLowToHigh(siena.core.base.ExtClassInfo, boolean)
	 */
	@SuppressWarnings("unchecked")
	@Override
	protected <T> Function<Map<String, String>, T> getFunctionFromLowToHigh(RedisClassInfo pCLInfo, boolean pOnlyKeys)
	{
		try
		{
			if (pOnlyKeys == true)
				return (Function<Map<String, String>, T>) sTransformerToObjectKeysMap.get(pCLInfo);
			else
				return (Function<Map<String, String>, T>) sTransformerToObjectMap.get(pCLInfo);
		}
		catch (ExecutionException ex)
		{
			throw Throwables.propagate(ex);
		}
	}

	/**
	 * @see siena.core.base.BasePersistenceManager#getFunctionFromLowToLowKey(siena.core.base.ExtClassInfo)
	 */
	@Override
	protected Function<Map<String, String>, Map<String, String>> getFunctionFromLowToLowKey(RedisClassInfo pCLInfo)
	{
		try
		{
			return (Function<Map<String, String>, Map<String, String>>) sTransformerToKeysMap.get(pCLInfo);
		}
		catch (ExecutionException ex)
		{
			throw Throwables.propagate(ex);
		}
	}

	/**
	 * @see siena.core.base.BasePersistenceManager#convertToRawLowColumnValue(siena.core.base.ExtClassInfo,
	 *      java.lang.reflect.Field, java.lang.Object)
	 */
	@Override
	protected Object convertToRawLowColumnValue(RedisClassInfo pCLInfo, Field pField, Object pValue)
	{
		return convertToString(pCLInfo, pField, pField.getType(), pValue);
	}

	/**
	 * @see siena.core.base.BasePersistenceManager#compareRawToLowColumnValue(java.lang.Object,
	 *      siena.core.base.ExtClassInfo, java.lang.reflect.Field, java.lang.Object)
	 */
	@Override
	protected int compareRawToLowColumnValue(Map<String, String> pObj, RedisClassInfo pCLInfo, Field pField,
		Object pRawValue)
	{
		String colName = pCLInfo.mFieldToColumnNameMap.get(pField);
		String ov = pObj.get(colName);
		String tv = (String) pRawValue;
		if ((ov == null) && (tv == null))
			return 0;
		if ((ov == null) && (tv != null))
			return 1;
		if ((ov != null) && (tv == null))
			return -1;
		return ov.compareTo(tv);
	}

	/**
	 * @see siena.core.base.BasePersistenceManager#getLowColumnValue(java.lang.Object, siena.core.base.ExtClassInfo,
	 *      java.lang.Class, java.lang.reflect.Field)
	 */
	@SuppressWarnings("unchecked")
	@Override
	protected <T> T getLowColumnValue(Map<String, String> pObj, RedisClassInfo pCLInfo, Class<T> pResultClass,
		Field pField)
	{
		String colName = pCLInfo.mFieldToColumnNameMap.get(pField);
		String v = pObj.get(colName);
		if ((v == null) || ((v != null) && ((pResultClass == Object.class) || (pResultClass.isInstance(v) == false))))
			return convertFromString(pCLInfo, pField, pResultClass, v);
		return (T) v;
	}

	/**
	 * @see siena.core.base.BasePersistenceManager#getLowColumnValue(java.lang.Object, siena.core.base.ExtClassInfo,
	 *      java.lang.Class, java.lang.String)
	 */
	@SuppressWarnings("unchecked")
	@Override
	protected <T> T getLowColumnValue(Map<String, String> pObj, RedisClassInfo pCLInfo, Class<T> pResultClass,
		String pColumnName)
	{
		String v = pObj.get(pColumnName);
		Field field = pCLInfo.mColumnNameToFieldMap.get(pColumnName);
		if ((v == null) || ((v != null) && ((pResultClass == Object.class) || (pResultClass.isInstance(v) == false))))
			return convertFromString(pCLInfo, field, pResultClass, v);
		return (T) v;
	}

	/**
	 * @see siena.core.base.BasePersistenceManager#setLowColumnValue(java.lang.Object, siena.core.base.ExtClassInfo,
	 *      java.lang.reflect.Field, java.lang.Object)
	 */
	@Override
	protected void setLowColumnValue(Map<String, String> pObj, RedisClassInfo pCLInfo, Field pField, Object pValue)
	{
		String colName = pCLInfo.mFieldToColumnNameMap.get(pField);
		pObj.put(colName, convertToString(pCLInfo, pField, pField.getType(), pValue));
	}

	/**
	 * @see siena.core.base.BasePersistenceManager#createLow(siena.core.base.ExtClassInfo)
	 */
	@Override
	protected Map<String, String> createLow(RedisClassInfo pCLInfo)
	{
		return new HashMap<String, String>();
	}

	/**
	 * @see siena.core.base.BasePersistenceManager#convertStringToLow(java.lang.String)
	 */
	@Override
	protected Map<String, String> convertStringToLow(String pValue)
	{
		Map<String, String> result = new HashMap<String, String>();
		for (StringTokenizer st = new StringTokenizer(pValue, "&"); st.hasMoreTokens();)
		{
			String token = st.nextToken();
			int offset = token.indexOf('=');
			String keyEnc = token.substring(0, offset);
			String valueEnc = token.substring(offset + 1);
			try
			{
				result.put(mStringEncoder.decode(keyEnc), mStringEncoder.decode(valueEnc));
			}
			catch (DecoderException ex)
			{
				throw new RuntimeException(ex);
			}
		}
		return result;
	}

	/**
	 * @see siena.core.base.BasePersistenceManager#convertLowToString(java.lang.Object)
	 */
	@Override
	protected String convertLowToString(Map<String, String> pObj)
	{
		StringBuilder sb = new StringBuilder();
		boolean isFirst = true;
		try
		{
			for (Map.Entry<String, String> pair : pObj.entrySet())
			{
				if (isFirst == true)
					isFirst = false;
				else
					sb.append('&');
				sb.append(mStringEncoder.encode(pair.getKey()));
				sb.append('=');
				sb.append(mStringEncoder.encode(pair.getValue()));
			}
		}
		catch (EncoderException ex)
		{
			throw new RuntimeException(ex);
		}
		return sb.toString();
	}

	/**
	 * @see siena.core.base.BasePersistenceManager#generateUUID(java.lang.Class)
	 */
	@SuppressWarnings("unchecked")
	@Override
	protected <T> T generateUUID(Class<T> pUUIDClass)
	{
		if (pUUIDClass == String.class)
			return (T) new UUID().toString();
		else if (pUUIDClass == UUID.class)
			return (T) new UUID();
		else if (pUUIDClass == java.util.UUID.class)
		{
			UUID aUUID = new UUID();
			return (T) new java.util.UUID(aUUID.time, aUUID.clockSeqAndNode);
		}
		else
			throw new RuntimeException("Unsupported ID type " + pUUIDClass.getName()); //$NON-NLS-1$
	}

	/**
	 * @see siena.core.base.BasePersistenceManager#save(java.lang.Object)
	 */
	@Override
	public void save(Object obj)
	{
		insert(obj);
	}

	/**
	 * @see siena.core.base.BasePersistenceManager#getLowColumnNames(java.lang.Object)
	 */
	@Override
	protected Set<String> getLowColumnNames(Map<String, String> pKey)
	{
		return pKey.keySet();
	}

	/**
	 * Redis doesn't support returning the results of a get until the transaction is complete
	 * 
	 * @see siena.core.base.BasePersistenceManager#supportsGetSetTransactions()
	 */
	@Override
	protected boolean supportsGetSetTransactions()
	{
		return false;
	}

}
