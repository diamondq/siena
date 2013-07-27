package siena.core.base;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Pattern;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.EncoderException;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.net.URLCodec;

import siena.AbstractPersistenceManager;
import siena.BaseQueryData;
import siena.ClassInfo;
import siena.DateTime;
import siena.Generator;
import siena.Id;
import siena.Json;
import siena.Query;
import siena.QueryFilter;
import siena.QueryFilterSearch;
import siena.QueryFilterSimple;
import siena.QueryJoin;
import siena.QueryOrder;
import siena.SienaException;
import siena.SimpleDate;
import siena.Time;
import siena.Util;
import siena.core.One4PM;
import siena.core.Polymorphic;
import siena.core.QueryFilterEmbedded;
import siena.core.async.PersistenceManagerAsync;
import siena.core.async.SienaFuture;
import siena.core.base.ExtTransScope.CallData;
import siena.core.options.QueryOptionOffset;
import siena.core.options.QueryOptionPage;
import siena.core.options.QueryOptionState;
import siena.embed.EmbedIgnore;
import siena.embed.Embedded;
import siena.embed.JsonSerializer;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;

public abstract class BasePersistenceManager<EXTCLASSINFO extends ExtClassInfo, EXTQUERYINFO extends BaseQueryInfo<?, EXTCLASSINFO>, EXTTRANSSCOPE extends ExtTransScope, EXTLOWOBJECT>
	extends AbstractPersistenceManager
{
	public static class MapData<EXTLOWOBJECT>
	{
		public String		name;
		public String		type;
		public EXTLOWOBJECT	key;
		public EXTLOWOBJECT	data;
		public String		className;
	}

	public static class SearchTerm
	{
		public String	term;
		public Pattern	pattern;
	}

	private static final Function<SienaFuture<Boolean>, SienaFuture<Boolean>>	sDeleteCheck				=
																												new CheckFuture<Boolean>(
																													Boolean.TRUE,
																													"Attempted delete of an non-existent object.");

	protected Function<EXTLOWOBJECT, EXTLOWOBJECT>								mNOPTransformers			=
																												Functions
																													.identity();
	private ConcurrentMap<Class<?>, Method>										mCachedMethods				=
																												new ConcurrentHashMap<Class<?>, Method>();
	protected URLCodec															mStringEncoder				=
																												new URLCodec(
																													"UTF-8");
	protected boolean															mIsNullLowValueSupported	= false;

	/* ********************************************************************************
	 * NOT IMPLEMENTED
	 */

	/**
	 * @see siena.PersistenceManager#async()
	 */
	@Override
	public <T> PersistenceManagerAsync async()
	{
		throw new IllegalArgumentException("Not yet implemented"); //$NON-NLS-1$
	}

	/* ********************************************************************************
	 * CALL PRIMITIVES FOR IMPLEMENTATION
	 */

	/**
	 * @see siena.PersistenceManager#update(java.lang.Object)
	 */
	@Override
	public void update(Object pObj)
	{
		EXTTRANSSCOPE transScope = openTransactionScope(true);
		try
		{
			EXTCLASSINFO clInfo = getClassInfo(pObj.getClass(), true);
			List<MapData<EXTLOWOBJECT>> data = convertToMap(clInfo, pObj, false);
			storeMapData(transScope, clInfo, data, false);
			if (transScope.transResult == TransResultType.ROLLBACK)
				transScope.transResult = TransResultType.COMMIT;
		}
		finally
		{
			closeTransactionScope(transScope);
		}
	}

	/**
	 * @see siena.PersistenceManager#insert(java.lang.Object)
	 */
	@Override
	public void insert(Object pObj)
	{
		EXTTRANSSCOPE transScope = openTransactionScope(true);
		try
		{
			EXTCLASSINFO clInfo = getClassInfo(pObj.getClass(), true);
			List<MapData<EXTLOWOBJECT>> data = convertToMap(clInfo, pObj, false);
			storeMapData(transScope, clInfo, data, true);
			if (transScope.transResult == TransResultType.ROLLBACK)
				transScope.transResult = TransResultType.COMMIT;
		}
		finally
		{
			closeTransactionScope(transScope);
		}

	}

	@Override
	public void init(Properties pP)
	{
	}

	public <T> int update(BaseQueryData<T> query, Map<String, ?> fieldValues)
	{
		EXTTRANSSCOPE transScope = openTransactionScope(true);
		try
		{
			EXTCLASSINFO clInfo = getClassInfo(query.getQueriedClass(), true);
			Iterable<T> iterable = iter(query, -1, null);
			int count = 0;
			for (T obj : iterable)
			{
				for (Entry<String, ?> pair : fieldValues.entrySet())
				{
					Field field = clInfo.mColumnNameToFieldMap.get(pair.getKey());

					Util.setField(obj, field, pair.getValue());
				}
				update(obj);
				count++;
			}
			if (transScope.transResult == TransResultType.ROLLBACK)
				transScope.transResult = (count == 0 ? TransResultType.READONLY : TransResultType.COMMIT);
			return count;
		}
		finally
		{
			closeTransactionScope(transScope);
		}
	}

	/**
	 * @see siena.PersistenceManager#insert(java.lang.Object[])
	 */
	@Override
	public int insert(Object... objects)
	{
		if (objects != null)
		{
			EXTTRANSSCOPE transScope = openTransactionScope(true);
			try
			{
				int count = 0;
				for (Object obj : objects)
				{
					insert(obj);
					count++;
				}
				if (transScope.transResult == TransResultType.ROLLBACK)
					transScope.transResult = (count == 0 ? TransResultType.READONLY : TransResultType.COMMIT);
				return objects.length;
			}
			finally
			{
				closeTransactionScope(transScope);
			}
		}
		return 0;
	}

	/**
	 * @see siena.PersistenceManager#insert(java.lang.Iterable)
	 */
	@Override
	public int insert(Iterable<?> objects)
	{
		if (objects != null)
		{
			EXTTRANSSCOPE transScope = openTransactionScope(true);
			try
			{
				int count = 0;
				for (Object obj : objects)
				{
					insert(obj);
					count++;
				}
				if (transScope.transResult == TransResultType.ROLLBACK)
					transScope.transResult = (count == 0 ? TransResultType.READONLY : TransResultType.COMMIT);
				return count;
			}
			finally
			{
				closeTransactionScope(transScope);
			}
		}
		return 0;
	}

	/**
	 * @see siena.PersistenceManager#save(java.lang.Object[])
	 */
	@Override
	public int save(Object... objects)
	{
		if (objects != null)
		{
			EXTTRANSSCOPE transScope = openTransactionScope(true);
			try
			{
				int count = 0;
				for (Object obj : objects)
				{
					save(obj);
					count++;
				}
				if (transScope.transResult == TransResultType.ROLLBACK)
					transScope.transResult = (count == 0 ? TransResultType.READONLY : TransResultType.COMMIT);
				return count;
			}
			finally
			{
				closeTransactionScope(transScope);
			}
		}
		return 0;
	}

	/**
	 * @see siena.PersistenceManager#save(java.lang.Iterable)
	 */
	@Override
	public int save(Iterable<?> objects)
	{
		if (objects != null)
		{
			EXTTRANSSCOPE transScope = openTransactionScope(true);
			try
			{
				int count = 0;
				for (Object obj : objects)
				{
					save(obj);
					count++;
				}
				if (transScope.transResult == TransResultType.ROLLBACK)
					transScope.transResult = (count == 0 ? TransResultType.READONLY : TransResultType.COMMIT);
				return count;
			}
			finally
			{
				closeTransactionScope(transScope);
			}
		}
		return 0;
	}

	/**
	 * @see siena.PersistenceManager#save(java.lang.Object)
	 */
	@Override
	public void save(Object obj)
	{
		throw new IllegalArgumentException("Not yet implemented"); //$NON-NLS-1$
	}

	public <T> int delete(BaseQueryData<T> pQuery)
	{
		EXTTRANSSCOPE transScope = openTransactionScope(true);
		try
		{
			EXTCLASSINFO clInfo = getClassInfo(pQuery.getQueriedClass(), true);
			EXTQUERYINFO queryInfo = expandQuery(clInfo, pQuery, true);
			Iterator<EXTLOWOBJECT> iterator =
				retrieveData(transScope, clInfo, pQuery, queryInfo, -1, null, getFunctionFromLowToLowKey(clInfo));
			int count = 0;
			for (; iterator.hasNext();)
			{
				EXTLOWOBJECT key = iterator.next();
				deleteByLowKey(clInfo, key);
				count++;
			}
			if (transScope.transResult == TransResultType.ROLLBACK)
				transScope.transResult = (count == 0 ? TransResultType.READONLY : TransResultType.COMMIT);
			return count;
		}
		finally
		{
			closeTransactionScope(transScope);
		}
	}

	public <T> int count(BaseQueryData<T> pQuery)
	{
		EXTTRANSSCOPE transScope = openTransactionScope(true);
		try
		{
			EXTCLASSINFO clInfo = getClassInfo(pQuery.getQueriedClass(), true);
			EXTQUERYINFO queryInfo = expandQuery(clInfo, pQuery, true);
			Iterator<EXTLOWOBJECT> iterator =
				retrieveData(transScope, clInfo, pQuery, queryInfo, -1, null, mNOPTransformers);
			int count = Iterators.size(iterator);
			if (transScope.transResult == TransResultType.ROLLBACK)
				transScope.transResult = TransResultType.READONLY;
			return count;
		}
		finally
		{
			closeTransactionScope(transScope);
		}
	}

	public <T> List<T> fetch(BaseQueryData<T> pQuery, int pLimit, Object pOffset)
	{
		EXTTRANSSCOPE transScope = openTransactionScope(true);
		try
		{
			EXTCLASSINFO clInfo = getClassInfo(pQuery.getQueriedClass(), true);
			EXTQUERYINFO queryInfo = expandQuery(clInfo, pQuery, false);
			Function<EXTLOWOBJECT, T> transformerToObject = getFunctionFromLowToHigh(clInfo, false);
			Iterator<T> iterator =
				retrieveData(transScope, clInfo, pQuery, queryInfo, pLimit, pOffset, transformerToObject);
			List<T> results = Lists.newArrayList(iterator);
			if (transScope.transResult == TransResultType.ROLLBACK)
				transScope.transResult = TransResultType.READONLY;
			return results;
		}
		finally
		{
			closeTransactionScope(transScope);
		}
	}

	public <T> Iterable<T> iterPerPage(BaseQueryData<T> pQuery, int pPageSize)
	{
		return iter(pQuery, -1, null);
	}

	public <T> Iterable<T> iter(BaseQueryData<T> pQuery, int pLimit, Object pOffset)
	{
		EXTTRANSSCOPE transScope = openTransactionScope(true);
		try
		{
			EXTCLASSINFO clInfo = getClassInfo(pQuery.getQueriedClass(), true);
			EXTQUERYINFO queryInfo = expandQuery(clInfo, pQuery, false);
			Function<EXTLOWOBJECT, T> transformerToObject = getFunctionFromLowToHigh(clInfo, false);
			Iterator<T> iterator =
				retrieveData(transScope, clInfo, pQuery, queryInfo, pLimit, pOffset, transformerToObject);
			ArrayList<T> list = Lists.newArrayList(iterator);
			if (transScope.transResult == TransResultType.ROLLBACK)
				transScope.transResult = TransResultType.READONLY;
			return list;
		}
		finally
		{
			closeTransactionScope(transScope);
		}
	}

	public <T> List<T> fetchKeys(BaseQueryData<T> pQuery, int pLimit, Object pOffset)
	{
		EXTTRANSSCOPE transScope = openTransactionScope(true);
		try
		{
			EXTCLASSINFO clInfo = getClassInfo(pQuery.getQueriedClass(), true);
			EXTQUERYINFO queryInfo = expandQuery(clInfo, pQuery, true);
			Function<EXTLOWOBJECT, T> transformerToObjectKeys = getFunctionFromLowToHigh(clInfo, true);
			Iterator<T> iterator =
				retrieveData(transScope, clInfo, pQuery, queryInfo, pLimit, pOffset, transformerToObjectKeys);
			List<T> results = Lists.newArrayList(iterator);
			if (transScope.transResult == TransResultType.ROLLBACK)
				transScope.transResult = TransResultType.READONLY;
			return results;
		}
		finally
		{
			closeTransactionScope(transScope);
		}
	}

	/**
	 * @see siena.PersistenceManager#delete(java.lang.Object[])
	 */
	@Override
	public int delete(Object... objects)
	{
		if (objects != null)
		{
			EXTTRANSSCOPE transScope = openTransactionScope(true);
			try
			{
				int count = 0;
				for (Object obj : objects)
				{
					delete(obj);
					count++;
				}
				if (transScope.transResult == TransResultType.ROLLBACK)
					transScope.transResult = (count == 0 ? TransResultType.READONLY : TransResultType.COMMIT);
				return count;
			}
			finally
			{
				closeTransactionScope(transScope);
			}
		}
		return 0;
	}

	/**
	 * @see siena.PersistenceManager#delete(java.lang.Iterable)
	 */
	@Override
	public int delete(Iterable<?> objects)
	{
		if (objects != null)
		{
			EXTTRANSSCOPE transScope = openTransactionScope(true);
			try
			{
				int count = 0;
				for (Object obj : objects)
				{
					delete(obj);
					count++;
				}
				if (transScope.transResult == TransResultType.ROLLBACK)
					transScope.transResult = (count == 0 ? TransResultType.READONLY : TransResultType.COMMIT);
				return count;
			}
			finally
			{
				closeTransactionScope(transScope);
			}
		}
		return 0;
	}

	/**
	 * @see siena.PersistenceManager#deleteByKeys(java.lang.Class, java.lang.Iterable)
	 */
	@Override
	public <T> int deleteByKeys(Class<T> clazz, Iterable<?> keys)
	{
		if (keys != null)
		{
			EXTTRANSSCOPE transScope = openTransactionScope(true);
			try
			{
				EXTCLASSINFO clInfo = getClassInfo(clazz, true);
				if (clInfo.mSienaInfo.keys.size() > 1)
					throw new IllegalArgumentException("Composite key models not supported via deleteByKeys");
				Field keyField = clInfo.mSienaInfo.keys.get(0);
				int count = 0;
				EXTLOWOBJECT keyObj = createLow(clInfo);
				for (Object key : keys)
				{
					setLowColumnValue(keyObj, clInfo, keyField, key);
					deleteByLowKey(clInfo, keyObj);
					count++;
				}
				if (transScope.transResult == TransResultType.ROLLBACK)
					transScope.transResult = (count == 0 ? TransResultType.READONLY : TransResultType.COMMIT);
				return count;
			}
			finally
			{
				closeTransactionScope(transScope);
			}
		}

		return 0;
	}

	/**
	 * @see siena.PersistenceManager#deleteByKeys(java.lang.Class, java.lang.Object[])
	 */
	@Override
	public <T> int deleteByKeys(Class<T> clazz, Object... keys)
	{
		if (keys != null)
		{
			EXTTRANSSCOPE transScope = openTransactionScope(true);
			try
			{
				EXTCLASSINFO clInfo = getClassInfo(clazz, true);
				if (clInfo.mSienaInfo.keys.size() > 1)
					throw new IllegalArgumentException("Composite key models not supported via deleteByKeys");
				Field keyField = clInfo.mSienaInfo.keys.get(0);
				EXTLOWOBJECT keyObj = createLow(clInfo);
				int count = 0;
				for (Object key : keys)
				{
					setLowColumnValue(keyObj, clInfo, keyField, key);
					deleteByLowKey(clInfo, keyObj);
					count++;
				}
				if (transScope.transResult == TransResultType.ROLLBACK)
					transScope.transResult = (count == 0 ? TransResultType.READONLY : TransResultType.COMMIT);
				return count;
			}
			finally
			{
				closeTransactionScope(transScope);
			}
		}
		return 0;
	}

	/**
	 * @see siena.PersistenceManager#get(java.lang.Object[])
	 */
	@Override
	public int get(Object... models)
	{
		if (models != null)
		{
			EXTTRANSSCOPE transScope = openTransactionScope(true);
			try
			{
				int count = 0;
				for (Object obj : models)
				{
					get(obj);
					count++;
				}
				if (transScope.transResult == TransResultType.ROLLBACK)
					transScope.transResult = TransResultType.READONLY;
				return count;
			}
			finally
			{
				closeTransactionScope(transScope);
			}
		}
		return 0;
	}

	/**
	 * @see siena.PersistenceManager#get(java.lang.Iterable)
	 */
	@Override
	public <T> int get(Iterable<T> models)
	{
		if (models != null)
		{
			EXTTRANSSCOPE transScope = openTransactionScope(true);
			try
			{
				int count = 0;
				for (Object obj : models)
				{
					get(obj);
					count++;
				}
				if (transScope.transResult == TransResultType.ROLLBACK)
					transScope.transResult = TransResultType.READONLY;
				return count;
			}
			finally
			{
				closeTransactionScope(transScope);
			}
		}
		return 0;
	}

	/**
	 * @see siena.PersistenceManager#getByKeys(java.lang.Class, java.lang.Object[])
	 */
	@Override
	public <T> List<T> getByKeys(Class<T> clazz, Object... keys)
	{
		List<T> results = new ArrayList<T>();
		if (keys != null)
		{
			EXTTRANSSCOPE transScope = openTransactionScope(true);
			try
			{
				for (Object key : keys)
					results.add(getByKey(clazz, key));
				if (transScope.transResult == TransResultType.ROLLBACK)
					transScope.transResult = TransResultType.READONLY;
			}
			finally
			{
				closeTransactionScope(transScope);
			}
		}
		return results;
	}

	/**
	 * @see siena.PersistenceManager#getByKeys(java.lang.Class, java.lang.Iterable)
	 */
	@Override
	public <T> List<T> getByKeys(Class<T> clazz, Iterable<?> keys)
	{
		List<T> results = new ArrayList<T>();
		if (keys != null)
		{
			EXTTRANSSCOPE transScope = openTransactionScope(true);
			try
			{
				for (Object key : keys)
					results.add(getByKey(clazz, key));
				if (transScope.transResult == TransResultType.ROLLBACK)
					transScope.transResult = TransResultType.READONLY;
			}
			finally
			{
				closeTransactionScope(transScope);
			}
		}
		return results;
	}

	/**
	 * @see siena.PersistenceManager#update(java.lang.Object[])
	 */
	@Override
	public <T> int update(Object... models)
	{
		if (models != null)
		{
			EXTTRANSSCOPE transScope = openTransactionScope(true);
			try
			{
				int count = 0;
				for (Object obj : models)
				{
					update(obj);
					count++;
				}
				if (transScope.transResult == TransResultType.ROLLBACK)
					transScope.transResult = (count == 0 ? TransResultType.READONLY : TransResultType.COMMIT);
				return count;
			}
			finally
			{
				closeTransactionScope(transScope);
			}
		}
		return 0;
	}

	/**
	 * @see siena.PersistenceManager#update(java.lang.Iterable)
	 */
	@Override
	public <T> int update(Iterable<T> models)
	{
		if (models != null)
		{
			EXTTRANSSCOPE transScope = openTransactionScope(true);
			try
			{
				int count = 0;
				for (Object obj : models)
				{
					update(obj);
					count++;
				}
				if (transScope.transResult == TransResultType.ROLLBACK)
					transScope.transResult = (count == 0 ? TransResultType.READONLY : TransResultType.COMMIT);
				return count;
			}
			finally
			{
				closeTransactionScope(transScope);
			}
		}
		return 0;
	}

	/**
	 * @see siena.PersistenceManager#supportedOperators()
	 */
	@Override
	public String[] supportedOperators()
	{
		return new String[] {"<", "<=", ">", ">=", "!=", "=", "IN"}; //$NON-NLS-1$//$NON-NLS-2$
	}

	/**
	 * @see siena.PersistenceManager#getByKey(java.lang.Class, java.lang.Object)
	 */
	@Override
	public <T> T getByKey(Class<T> clazz, Object key)
	{
		EXTTRANSSCOPE transScope = openTransactionScope(true);
		try
		{
			EXTCLASSINFO classInfo = getClassInfo(clazz, true);
			List<Field> keys = classInfo.mSienaInfo.keys;
			Query<T> q = createQuery(clazz);
			for (Field f : keys)
			{
				String keyName = classInfo.mFieldToColumnNameMap.get(f);
				q = q.filter(keyName, key);
			}
			T r = q.get();
			if (transScope.transResult == TransResultType.ROLLBACK)
				transScope.transResult = TransResultType.READONLY;
			return r;
		}
		finally
		{
			closeTransactionScope(transScope);
		}
	}

	/**
	 * @see siena.PersistenceManager#beginTransaction(int)
	 */
	@Override
	public void beginTransaction(int isolationLevel)
	{
		beginTransaction();
	}

	/**
	 * @see siena.PersistenceManager#beginTransaction()
	 */
	@Override
	public void beginTransaction()
	{
		openTransactionScope(false);
	}

	/**
	 * @see siena.PersistenceManager#rollbackTransaction()
	 */
	@Override
	public void rollbackTransaction()
	{
		EXTTRANSSCOPE scope = openTransactionScope(true);
		scope.keepOpen = false;
		scope.transResult = TransResultType.ROLLBACK;
		closeTransactionScope(scope);
	}

	/**
	 * @see siena.PersistenceManager#commitTransaction()
	 */
	@Override
	public void commitTransaction()
	{
		EXTTRANSSCOPE scope = openTransactionScope(true);
		scope.keepOpen = false;
		scope.transResult = TransResultType.COMMIT;
		closeTransactionScope(scope);
	}

	/**
	 * @see siena.PersistenceManager#closeConnection()
	 */
	@Override
	public void closeConnection()
	{
	}

	/**
	 * @see siena.PersistenceManager#fetch(siena.Query)
	 */
	@Override
	public <T> List<T> fetch(Query<T> query)
	{
		@SuppressWarnings("unchecked")
		BaseQueryData<T> queryData = (BaseQueryData<T>) query;
		return fetch(queryData, -1, null);
	}

	/**
	 * @see siena.core.base.BasePersistenceManager#fetch(siena.Query, int, java.lang.Object)
	 */
	@Override
	public <T> List<T> fetch(Query<T> pQuery, int pLimit, Object pOffset)
	{
		@SuppressWarnings("unchecked")
		BaseQueryData<T> queryData = (BaseQueryData<T>) pQuery;
		return fetch(queryData, pLimit, pOffset);
	}

	/**
	 * @see siena.PersistenceManager#fetch(siena.Query, int)
	 */
	@Override
	public <T> List<T> fetch(Query<T> query, int limit)
	{
		@SuppressWarnings("unchecked")
		BaseQueryData<T> queryData = (BaseQueryData<T>) query;
		return fetch(queryData, limit, null);
	}

	/**
	 * @see siena.PersistenceManager#count(siena.Query)
	 */
	@Override
	public <T> int count(Query<T> pQuery)
	{
		@SuppressWarnings("unchecked")
		BaseQueryData<T> queryData = (BaseQueryData<T>) pQuery;
		return count(queryData);
	}

	/**
	 * @see siena.PersistenceManager#fetchKeys(siena.Query)
	 */
	@Override
	public <T> List<T> fetchKeys(Query<T> query)
	{
		return fetchKeys(query, -1, null);
	}

	/**
	 * @see siena.PersistenceManager#fetchKeys(siena.Query, int)
	 */
	@Override
	public <T> List<T> fetchKeys(Query<T> query, int limit)
	{
		return fetchKeys(query, limit, null);
	}

	/**
	 * @see siena.PersistenceManager#iter(siena.Query)
	 */
	@Override
	public <T> Iterable<T> iter(Query<T> query)
	{
		return iter(query, -1, null);
	}

	/**
	 * @see siena.PersistenceManager#iter(siena.Query, int)
	 */
	@Override
	public <T> Iterable<T> iter(Query<T> query, int limit)
	{
		return iter(query, limit, null);
	}

	/**
	 * @see siena.PersistenceManager#delete(siena.Query)
	 */
	@Override
	public <T> int delete(Query<T> pQuery)
	{
		@SuppressWarnings("unchecked")
		BaseQueryData<T> queryData = (BaseQueryData<T>) pQuery;
		return delete(queryData);
	}

	/**
	 * @see siena.PersistenceManager#update(siena.Query, java.util.Map)
	 */
	@Override
	public <T> int update(Query<T> pQuery, Map<String, ?> pFieldValues)
	{
		@SuppressWarnings("unchecked")
		BaseQueryData<T> queryData = (BaseQueryData<T>) pQuery;
		return update(queryData, pFieldValues);
	}

	/**
	 * @see siena.PersistenceManager#fetchKeys(siena.Query, int, java.lang.Object)
	 */
	@Override
	public <T> List<T> fetchKeys(Query<T> pQuery, int pLimit, Object pOffset)
	{
		@SuppressWarnings("unchecked")
		BaseQueryData<T> queryData = (BaseQueryData<T>) pQuery;
		return fetchKeys(queryData, pLimit, pOffset);
	}

	/**
	 * @see siena.PersistenceManager#iter(siena.Query, int, java.lang.Object)
	 */
	@Override
	public <T> Iterable<T> iter(Query<T> pQuery, int pLimit, Object pOffset)
	{
		@SuppressWarnings("unchecked")
		BaseQueryData<T> queryData = (BaseQueryData<T>) pQuery;
		return iter(queryData, pLimit, pOffset);
	}

	/**
	 * @see siena.PersistenceManager#paginate(siena.Query)
	 */
	@Override
	public <T> void paginate(Query<T> pQuery)
	{
		@SuppressWarnings("unchecked")
		BaseQueryData<T> queryData = (BaseQueryData<T>) pQuery;
		paginate(queryData);
	}

	/**
	 * @see siena.PersistenceManager#nextPage(siena.Query)
	 */
	@Override
	public <T> void nextPage(Query<T> pQuery)
	{
		@SuppressWarnings("unchecked")
		BaseQueryData<T> queryData = (BaseQueryData<T>) pQuery;
		nextPage(queryData);
	}

	/**
	 * @see siena.PersistenceManager#previousPage(siena.Query)
	 */
	@Override
	public <T> void previousPage(Query<T> pQuery)
	{
		@SuppressWarnings("unchecked")
		BaseQueryData<T> queryData = (BaseQueryData<T>) pQuery;
		previousPage(queryData);
	}

	public <T> void paginate(BaseQueryData<T> query)
	{
		QueryOptionCursorDetails cursorOpt = (QueryOptionCursorDetails) query.option(QueryOptionCursorDetails.ID);
		QueryOptionState stateOpt = (QueryOptionState) query.option(QueryOptionState.ID);
		if (stateOpt.isStateless())
			cursorOpt.cursorOffset = 0;
	}

	public <T> void nextPage(BaseQueryData<T> query)
	{
		QueryOptionCursorDetails cursorOpt = (QueryOptionCursorDetails) query.option(QueryOptionCursorDetails.ID);
		QueryOptionPage pageOpt = (QueryOptionPage) query.option(QueryOptionPage.ID);

		if (pageOpt.isPaginating() == false)
			throw new SienaException("Issuing a nextPage is illegal when a paginated query isn't running");

		if (cursorOpt.isAtEnd == true)
			return;

		if (cursorOpt.isAtBeginning == true)
		{
			cursorOpt.isAtBeginning = false;
			return;
		}

		cursorOpt.cursorLimit = pageOpt.pageSize;
		cursorOpt.cursorOffset += pageOpt.pageSize;
	}

	public <T> void previousPage(BaseQueryData<T> query)
	{
		QueryOptionCursorDetails cursorOpt = (QueryOptionCursorDetails) query.option(QueryOptionCursorDetails.ID);
		QueryOptionPage pageOpt = (QueryOptionPage) query.option(QueryOptionPage.ID);

		if (pageOpt.isPaginating() == false)
			throw new SienaException("Issuing a previousPage is illegal when a paginated query isn't running");

		if (cursorOpt.isAtBeginning == true)
			return;

		if (cursorOpt.isAtEnd)
			cursorOpt.isAtEnd = false;

		cursorOpt.cursorLimit = pageOpt.pageSize;
		if (cursorOpt.cursorOffset >= pageOpt.pageSize)
			cursorOpt.cursorOffset -= pageOpt.pageSize;
		else
		{
			cursorOpt.cursorOffset = 0;
			cursorOpt.isAtBeginning = true;
		}
	}

	/**
	 * @see siena.PersistenceManager#delete(java.lang.Object)
	 */
	@Override
	public void delete(Object obj)
	{
		EXTTRANSSCOPE transScope = openTransactionScope(true);
		try
		{
			EXTCLASSINFO clInfo = getClassInfo(obj.getClass(), true);

			/* Retrieve the keys from the current object */

			EXTLOWOBJECT keys = getKeysOfObject(clInfo, obj);

			SienaFuture<Boolean> future = deleteByLowKey(clInfo, keys);
			addCallbackAfterSuccessfulTransaction(transScope, sDeleteCheck, future);

			if (transScope.transResult == TransResultType.ROLLBACK)
				transScope.transResult = TransResultType.COMMIT;
		}
		finally
		{
			closeTransactionScope(transScope);
		}
	}

	/**
	 * @see siena.PersistenceManager#get(java.lang.Object)
	 */
	@Override
	public void get(Object obj)
	{
		EXTTRANSSCOPE transScope = openTransactionScope(true);
		try
		{
			EXTCLASSINFO clInfo = getClassInfo(obj.getClass(), true);

			/* Retrieve the keys from the current object */

			EXTLOWOBJECT keys = getKeysOfObject(clInfo, obj);

			/* Load the raw data from the datastore based on the keys */

			EXTLOWOBJECT raw = loadLowFromLowKeys(clInfo, keys);
			if (raw == null)
				throw new SienaException("No such object at key " + keys.toString());

			/* Load the raw information into the current object */

			loadFromRaw(clInfo, raw, false, false, obj);

			if (transScope.transResult == TransResultType.ROLLBACK)
				transScope.transResult = TransResultType.READONLY;
		}
		finally
		{
			closeTransactionScope(transScope);
		}
	}

	/**
	 * @see siena.AbstractPersistenceManager#createQuery(java.lang.Class)
	 */
	@Override
	public <T> Query<T> createQuery(Class<T> pClazz)
	{
		return new ExtBaseQuery<T>(this, pClazz);
	}

	/**
	 * @see siena.AbstractPersistenceManager#createQuery(siena.BaseQueryData)
	 */
	@Override
	public <T> Query<T> createQuery(BaseQueryData<T> pData)
	{
		return new ExtBaseQuery<T>(this, pData);
	}

	/* ********************************************************************************
	 * Utilities
	 */

	/**
	 * This method returns an iterator for the given source of data where the data has been transformed and then sorted
	 * 
	 * @param pSource the original source of data
	 * @param pQuery the query
	 * @param pQueryInfo the query info
	 * @param pTransformer the transformer
	 * @param pAlreadyHandledOffsetAndLimit true if the offset and limit have already been processed in the source
	 * @param pBaseOptions the base options
	 * @return the iterator of sorted, transformed data
	 */
	@SuppressWarnings("unchecked")
	protected <O, T> Iterator<O> buildSortedIterator(Iterable<EXTLOWOBJECT> pSource, BaseQueryData<T> pQuery,
		final EXTCLASSINFO pCLInfo, EXTQUERYINFO pQueryInfo, Function<EXTLOWOBJECT, O> pTransformer,
		boolean pAlreadyHandledOffsetAndLimit, BaseOptions pBaseOptions)
	{
		final List<QueryOrder> orderBys = pQuery.getOrders();
		final List<QueryJoin> joins = pQuery.getJoins();
		Iterable<EXTLOWOBJECT> inputList = pSource;
		if ((orderBys != null) && (orderBys.isEmpty() == false))
		{
			Ordering<EXTLOWOBJECT> o = new Ordering<EXTLOWOBJECT>()
			{

				@Override
				public int compare(EXTLOWOBJECT left, EXTLOWOBJECT right)
				{
					for (QueryOrder qo : orderBys)
					{
						Object obj0;
						Object obj1;

						/*
						 * If there is a parent field in the order by, then we need to get the parent field, then
						 * resolve to a child
						 */

						if (qo.parentField != null)
						{
							obj0 =
								BasePersistenceManager.this.getLowColumnValue(left, pCLInfo, qo.parentField.getType(),
									qo.parentField);
							obj1 =
								BasePersistenceManager.this.getLowColumnValue(right, pCLInfo, qo.parentField.getType(),
									qo.parentField);
							for (QueryJoin qj : joins)
							{
								/* See if this parent field is one of the forced joins */

								if (qj.field == qo.parentField)
								{
									if ((obj0 != null) && (ClassInfo.isModel(qj.field.getType())))
									{
										EXTCLASSINFO newCLInfo = getClassInfo(qj.field.getType(), true);
										EXTLOWOBJECT childRawObj = (EXTLOWOBJECT) obj0;
										if (ClassInfo.isEmbedded(qj.field))
										{
											obj0 = convertFromRaw(newCLInfo, childRawObj, false, true);
										}
										else if (ClassInfo.isJoined(qj.field))
										{
											obj0 = convertFromRaw(newCLInfo, childRawObj, true, false);
											get(obj0);
										}
										else
										{
											/* Because this is a forced join, we'll tell it to full load anyway */
											obj0 = convertFromRaw(newCLInfo, childRawObj, true, false);
											get(obj0);
										}
									}
									if ((obj1 != null) && (ClassInfo.isModel(qj.field.getType())))
									{
										EXTCLASSINFO newCLInfo = getClassInfo(qj.field.getType(), true);
										EXTLOWOBJECT childRawObj = (EXTLOWOBJECT) obj1;
										if (ClassInfo.isEmbedded(qj.field))
										{
											obj1 = convertFromRaw(newCLInfo, childRawObj, false, true);
										}
										else if (ClassInfo.isJoined(qj.field))
										{
											obj1 = convertFromRaw(newCLInfo, childRawObj, true, false);
											get(obj1);
										}
										else
										{
											/* Because this is a forced join, we'll tell it to full load anyway */
											obj1 = convertFromRaw(newCLInfo, childRawObj, true, false);
											get(obj1);
										}
									}
									break;
								}
							}

							/* Now get the child value */

							obj0 = Util.readField(obj0, qo.field);
							obj1 = Util.readField(obj1, qo.field);
						}
						else
						{
							obj0 =
								BasePersistenceManager.this.getLowColumnValue(left, pCLInfo, qo.field.getType(),
									qo.field);
							obj1 =
								BasePersistenceManager.this.getLowColumnValue(right, pCLInfo, qo.field.getType(),
									qo.field);
						}
						if (obj0 == null)
						{
							if (obj1 != null)
								return qo.ascending == true ? -1 : 1;
						}
						else
						{
							if (obj1 == null)
								return qo.ascending == true ? 1 : -1;
							@SuppressWarnings("rawtypes")
							int r = ((Comparable) obj0).compareTo(obj1);
							if (r != 0)
								return qo.ascending == true ? r : -1 * r;
						}
					}
					return 0;
				}

			};
			inputList = o.sortedCopy(inputList);
		}

		/* If the offset and limits haven't already been handled, then enforce them now */

		if (pAlreadyHandledOffsetAndLimit == false)
		{
			if (pBaseOptions.cursorOpt.cursorOffset > 0)
				inputList = Iterables.skip(inputList, pBaseOptions.cursorOpt.cursorOffset);
			if (pBaseOptions.cursorOpt.cursorLimit < Integer.MAX_VALUE)
				inputList = Iterables.limit(inputList, pBaseOptions.cursorOpt.cursorLimit);
		}

		if (pBaseOptions.pageOpt.isPaginating())
		{
			if (inputList.iterator().hasNext() == false)
				pBaseOptions.cursorOpt.isAtEnd = true;
			else
				pBaseOptions.cursorOpt.isAtEnd = false;
		}
		else if (pBaseOptions.stateOpt.isStateful())
		{
			pBaseOptions.cursorOpt.cursorOffset += Iterables.size(inputList);
		}

		if (pBaseOptions.stateOpt.isStateful())
			pBaseOptions.cursorOpt.activate();

		Iterator<O> oIterator = Iterators.transform(inputList.iterator(), pTransformer);
		if (joins.isEmpty() == false)
			oIterator =
				Iterators.transform(oIterator,
					new LoadJoinsFunction<EXTCLASSINFO, EXTQUERYINFO, EXTTRANSSCOPE, EXTLOWOBJECT, O>(this, joins));
		return oIterator;

	}

	public List<MapData<EXTLOWOBJECT>> convertToMap(EXTCLASSINFO clInfo, Object obj, boolean pIsEmbedded)
	{
		List<MapData<EXTLOWOBJECT>> results = new ArrayList<MapData<EXTLOWOBJECT>>();
		MapData<EXTLOWOBJECT> primary = new MapData<EXTLOWOBJECT>();
		results.add(primary);
		primary.name = clInfo.mSienaInfo.tableName;
		primary.type = "table"; //$NON-NLS-1$
		primary.data = createLow(clInfo);
		primary.className = clInfo.mSienaInfo.clazz.getName();

		for (Field f : clInfo.mSienaInfo.allExtendedFields)
		{
			if (pIsEmbedded == true)
			{
				EmbedIgnore embed = f.getAnnotation(EmbedIgnore.class);
				if (embed != null)
					continue;
			}

			Object value = Util.readField(obj, f);

			/* Handle ID fields */

			if (ClassInfo.isId(f) == true)
			{
				Generator generator = f.getAnnotation(Id.class).value();
				if (generator == Generator.UUID)
				{
					/* If the UUID is not set, then create one now */

					if (value == null)
					{
						value = generateUUID(f.getType());

						/* Update the original object with the new UUID */

						Util.setField(obj, f, value);
					}
				}
				else if ((generator == Generator.AUTO_INCREMENT) || (generator == Generator.SEQUENCE))
				{
					boolean inc = false;
					if (value == null)
						inc = true;
					else if (value instanceof Short)
						inc = ((Short) value).shortValue() == 0 ? true : false;
					else if (value instanceof Integer)
						inc = ((Integer) value).intValue() == 0 ? true : false;
					else if (value instanceof Long)
						inc = ((Long) value).longValue() == 0 ? true : false;
					if (inc == true)
					{
						long idOffset = generateNextSequence(clInfo, clInfo.mFieldToColumnNameMap.get(f));

						/* Update the original object with the new UUID */

						Util.setField(obj, f, idOffset);
						value = idOffset;
					}
				}
				if (primary.key == null)
					primary.key = createLow(clInfo);
				setLowColumnValue(primary.key, clInfo, f, value);
			}
			else if (ClassInfo.isOwned(f))
			{
				/* All new children need to be persisted */

				if (ClassInfo.isOne(f))
				{
					One4PM<?> one = (One4PM<?>) value;
					if (one.isModified())
					{
						Object oneObj = one.get();
						EXTCLASSINFO newCLInfo = getClassInfo(oneObj.getClass(), true);
						List<MapData<EXTLOWOBJECT>> newMapList = convertToMap(newCLInfo, oneObj, false);
						results.addAll(newMapList);
						value = null;
					}
				}
				else
					throw new IllegalArgumentException("Unsupported owned relationship " + f.getType().getName());

			}
			if ((value != null) || ((value == null) && (mIsNullLowValueSupported == true)))
				setLowColumnValue(primary.data, clInfo, f, value);
		}

		return results;
	}

	/**
	 * Returns the keys of the given object. This is a Map of keys
	 * 
	 * @param value the object
	 * @param obj
	 * @return the key values
	 */
	protected EXTLOWOBJECT getKeysOfObject(EXTCLASSINFO pCLInfo, Object value)
	{
		EXTLOWOBJECT r = createLow(pCLInfo);
		for (Field f : pCLInfo.mSienaInfo.keys)
			setLowColumnValue(r, pCLInfo, f, Util.readField(value, f));
		return r;
	}

	protected String collapseKeyToString(EXTLOWOBJECT keyObj, EXTCLASSINFO pCLInfo, String pPrefix)
	{
		StringBuilder sb = new StringBuilder();
		boolean initialSeparator = false;
		if (pPrefix != null)
		{
			sb.append(pPrefix);
			initialSeparator = true;
		}

		for (String key : getLowColumnNames(keyObj))
		{
			if (initialSeparator == false)
				initialSeparator = true;
			else
				sb.append('&');
			sb.append(key);
			sb.append('=');
			sb.append(getLowColumnValue(keyObj, pCLInfo, String.class, key));
		}
		return sb.toString();
	}

	protected <T> EXTQUERYINFO expandQuery(EXTCLASSINFO pCLInfo, BaseQueryData<T> pQuery, boolean pKeysOnly)
	{
		return createQueryInfo(pCLInfo, pQuery, pKeysOnly);
	}

	protected <T> T convertFromRaw(EXTCLASSINFO clInfo, EXTLOWOBJECT pRawData, boolean pOnlyKeys, boolean pIsEmbedded)
	{
		try
		{
			@SuppressWarnings("unchecked")
			T newObj = (T) clInfo.mSienaInfo.clazz.newInstance();
			loadFromRaw(clInfo, pRawData, pOnlyKeys, pIsEmbedded, newObj);
			return newObj;
		}
		catch (InstantiationException e)
		{
			throw new RuntimeException(e);
		}
		catch (IllegalAccessException e)
		{
			throw new RuntimeException(e);
		}
	}

	protected <T> void loadFromRaw(EXTCLASSINFO clInfo, EXTLOWOBJECT pRawData, boolean pOnlyKeys, boolean pIsEmbedded,
		T newObj)
	{
		List<Field> fields;
		if (pOnlyKeys == true)
			fields = clInfo.mSienaInfo.keys;
		else
			fields = clInfo.mSienaInfo.allExtendedFields;
		for (Field f : fields)
		{
			if (pIsEmbedded == true)
			{
				EmbedIgnore embed = f.getAnnotation(EmbedIgnore.class);
				if (embed != null)
					continue;
			}
			Object value = getLowColumnValue(pRawData, clInfo, f.getType(), f);

			/*
			 * Determine if we have to convert the data from the raw into a new format
			 */

			if ((value != null) && (ClassInfo.isModel(f.getType())))
			{
				EXTCLASSINFO newCLInfo = getClassInfo(f.getType(), true);
				@SuppressWarnings("unchecked")
				EXTLOWOBJECT childRawObj = (EXTLOWOBJECT) value;
				if (ClassInfo.isEmbedded(f))
				{
					/* If it's embedded, then it's already been converted back to the real object */
				}
				else if (ClassInfo.isJoined(f))
				{
					value = convertFromRaw(newCLInfo, childRawObj, true, false);
					get(value);
				}
				else
				{
					value = convertFromRaw(newCLInfo, childRawObj, true, false);
				}
			}
			Util.setField(newObj, f, value);
		}
	}

	/**
	 * Checks to see if a given object in raw format matches the values in the query
	 * 
	 * @param pObj the object in raw format
	 * @param pCLInfo the class
	 * @param pQuery the query
	 * @param pQueryInfo the query info
	 * @return true or false
	 */
	protected boolean checkAgainstQuery(EXTLOWOBJECT pObj, EXTCLASSINFO pCLInfo, EXTQUERYINFO pQueryInfo)
	{
		List<QueryFilter> filters = pQueryInfo.mQuery.getFilters();
		for (QueryFilter f : filters)
		{
			if (f instanceof QueryFilterSimple)
			{
				QueryFilterSimple qfs = (QueryFilterSimple) f;
				if (checkAgainstSimpleQuery(pObj, pCLInfo, qfs.field, qfs.operator, qfs.value) == false)
					return false;
			}
			else if (f instanceof QueryFilterSearch)
			{
				QueryFilterSearch qfs = (QueryFilterSearch) f;
				SearchTerm[] searchTerms = pQueryInfo.mSearchMap.get(qfs);
				boolean match = false;
				for (String fieldName : qfs.fields)
				{
					Field field = pCLInfo.mColumnNameToFieldMap.get(fieldName);
					Object rawValue = getLowColumnValue(pObj, pCLInfo, field.getType(), field);
					if (rawValue != null)
					{
						String rawValueAsString = convertToString(pCLInfo, field, field.getType(), rawValue);
						for (SearchTerm term : searchTerms)
						{
							if (term.term != null)
								if (term.term.equals(rawValueAsString))
								{
									match = true;
									break;
								}
							if (term.pattern != null)
								if (term.pattern.matcher(rawValueAsString).matches() == true)
								{
									match = true;
									break;
								}
						}
					}
					if (match == true)
						break;
				}
				if (match == false)
					return false;
			}
			else if (f instanceof QueryFilterEmbedded)
			{
				QueryFilterEmbedded qfe = (QueryFilterEmbedded) f;
				if (qfe.fields.size() > 1)
				{
					Field topField = qfe.fields.get(0);
					Object value = getLowColumnValue(pObj, pCLInfo, topField.getType(), topField);
					if (value == null)
						return false;
					if (qfe.fields.size() > 2)
						for (int counter = 1; counter < qfe.fields.size() - 1; counter++)
						{
							Field field = qfe.fields.get(counter);

							value = Util.readField(value, field);
							if (value == null)
								return false;
						}
					Field bottomField = qfe.fields.get(qfe.fields.size() - 1);
					if (checkNativeAgainstSimpleQuery(value, bottomField, qfe.operator, qfe.value) == false)
						return false;
				}
				else
				{
					Field singleField = qfe.fields.get(0);
					if (checkAgainstSimpleQuery(pObj, pCLInfo, singleField, qfe.operator, qfe.value) == false)
						return false;
				}
			}
			else
				throw new IllegalArgumentException("Filter of type " + f.getClass().getName() + " not implemented.");
		}
		return true;
	}

	/**
	 * Perform a simple check against a single field
	 * 
	 * @param pObj the object to search
	 * @param pCLInfo the class info of the object
	 * @param queryField the field
	 * @param queryOperator the operator
	 * @param queryValue the value to test
	 * @return true if it matches or false if it does not
	 */
	protected boolean checkAgainstSimpleQuery(EXTLOWOBJECT pObj, EXTCLASSINFO pCLInfo, Field queryField,
		String queryOperator, Object queryValue)
	{
		Object rawValue = getLowColumnValue(pObj, pCLInfo, queryField.getType(), queryField);
		if (queryOperator.equals("="))
		{
			Object rawTestValue = convertToRawLowColumnValue(pCLInfo, queryField, queryValue);
			if (compareRawToLowColumnValue(pObj, pCLInfo, queryField, rawTestValue) != 0)
				return false;
		}
		else if (queryOperator.equals("!="))
		{
			Object rawTestValue = convertToRawLowColumnValue(pCLInfo, queryField, queryValue);
			if (compareRawToLowColumnValue(pObj, pCLInfo, queryField, rawTestValue) == 0)
				return false;
		}
		else if (queryOperator.equals("IN"))
		{
			if (queryValue instanceof Iterable)
			{
				boolean match = false;
				@SuppressWarnings("unchecked")
				Iterable<Object> io = (Iterable<Object>) queryValue;
				for (Object o : io)
				{
					Object rawTestValue = convertToRawLowColumnValue(pCLInfo, queryField, o);
					if (compareRawToLowColumnValue(pObj, pCLInfo, queryField, rawTestValue) == 0)
					{
						match = true;
						break;
					}
				}
				if (match == false)
					return false;
			}
			else
				throw new IllegalArgumentException(
					"Only Iterable's are supported values for a filter using the IN keyword. Found a "
						+ queryValue.getClass().getName());
		}
		else if (queryOperator.equals("<") || queryOperator.equals("<=") || queryOperator.equals(">")
			|| queryOperator.equals(">="))
		{
			int r;
			if ((rawValue instanceof Number) || (queryValue instanceof Number))
			{
				Number num1;
				Number num2;
				if (rawValue instanceof Number)
					num1 = (Number) rawValue;
				else if (rawValue instanceof String)
				{
					num1 = Double.parseDouble((String) rawValue);
				}
				else
					return false;
				if (queryValue instanceof Number)
					num2 = (Number) queryValue;
				else if (queryValue instanceof String)
				{
					num2 = Double.parseDouble((String) queryValue);
				}
				else
					return false;

				if (num1.getClass().equals(num2.getClass()))
				{
					@SuppressWarnings("unchecked")
					Comparable<Number> c1 = (Comparable<Number>) num1;
					r = c1.compareTo(num2);
				}
				else
				{
					if ((num1 instanceof BigDecimal) || (num2 instanceof BigDecimal) || (num1 instanceof BigInteger)
						|| (num2 instanceof BigInteger))
					{
						BigDecimal bd1;
						BigDecimal bd2;
						if (num1 instanceof BigDecimal)
							bd1 = (BigDecimal) num1;
						else if (num1 instanceof BigInteger)
							bd1 = new BigDecimal((BigInteger) num1);
						else if (num1 instanceof Byte)
							bd1 = new BigDecimal(num1.byteValue());
						else if (num1 instanceof Double)
							bd1 = new BigDecimal(num1.doubleValue());
						else if (num1 instanceof Float)
							bd1 = new BigDecimal(num1.floatValue());
						else if (num1 instanceof Integer)
							bd1 = new BigDecimal(num1.intValue());
						else if (num1 instanceof Long)
							bd1 = new BigDecimal(num1.longValue());
						else if (num1 instanceof Short)
							bd1 = new BigDecimal(num1.shortValue());
						else
							throw new IllegalStateException();
						if (num2 instanceof BigDecimal)
							bd2 = (BigDecimal) num2;
						else if (num2 instanceof BigInteger)
							bd2 = new BigDecimal((BigInteger) num2);
						else if (num2 instanceof Byte)
							bd2 = new BigDecimal(num2.byteValue());
						else if (num2 instanceof Double)
							bd2 = new BigDecimal(num2.doubleValue());
						else if (num2 instanceof Float)
							bd2 = new BigDecimal(num2.floatValue());
						else if (num2 instanceof Integer)
							bd2 = new BigDecimal(num2.intValue());
						else if (num2 instanceof Long)
							bd2 = new BigDecimal(num2.longValue());
						else if (num2 instanceof Short)
							bd2 = new BigDecimal(num2.shortValue());
						else
							throw new IllegalStateException();
						r = bd1.compareTo(bd2);
					}
					else if ((num1 instanceof Float) || (num1 instanceof Double) || (num2 instanceof Float)
						|| (num2 instanceof Double))
					{
						Double d1;
						Double d2;
						if (num1 instanceof Double)
							d1 = (Double) num1;
						else
							d1 = new Double(num1.doubleValue());
						if (num2 instanceof Double)
							d2 = (Double) num2;
						else
							d2 = new Double(num2.doubleValue());
						r = d1.compareTo(d2);
					}
					else
					{
						Long l1;
						Long l2;
						if (num1 instanceof Long)
							l1 = (Long) num1;
						else
							l1 = new Long(num1.longValue());
						if (num2 instanceof Long)
							l2 = (Long) num2;
						else
							l2 = new Long(num2.longValue());
						r = l1.compareTo(l2);
					}
				}
			}
			else
			{
				String s1 = rawValue.toString();
				String s2 = queryValue.toString();
				r = s1.compareTo(s2);
			}
			if (queryOperator.equals("<"))
			{
				if (r >= 0)
					return false;
			}
			else if (queryOperator.equals("<="))
			{
				if (r > 0)
					return false;
			}
			else if (queryOperator.equals(">"))
			{
				if (r <= 0)
					return false;
			}
			else if (queryOperator.equals(">="))
			{
				if (r < 0)
					return false;
			}
		}
		return true;
	}

	/**
	 * Perform a simple check against a single field
	 * 
	 * @param pObj the native object to search
	 * @param queryField the field
	 * @param queryOperator the operator
	 * @param queryValue the value to test
	 * @return true if it matches or false if it does not
	 */
	protected boolean checkNativeAgainstSimpleQuery(Object pObj, Field queryField, String queryOperator,
		Object queryValue)
	{
		if (pObj == null)
			return false;
		Object rawValue = Util.readField(pObj, queryField);
		if (queryOperator.equals("="))
		{
			if (queryValue.equals(rawValue) == false)
				return false;
		}
		else if (queryOperator.equals("!="))
		{
			if (queryValue.equals(rawValue) == true)
				return false;
		}
		else if (queryOperator.equals("IN"))
		{
			if (queryValue instanceof Iterable)
			{
				boolean match = false;
				@SuppressWarnings("unchecked")
				Iterable<Object> io = (Iterable<Object>) queryValue;
				for (Object o : io)
				{
					if (o.equals(rawValue))
					{
						match = true;
						break;
					}
				}
				if (match == false)
					return false;
			}
			else
				throw new IllegalArgumentException(
					"Only Iterable's are supported values for a filter using the IN keyword. Found a "
						+ queryValue.getClass().getName());
		}
		else if (queryOperator.equals("<") || queryOperator.equals("<=") || queryOperator.equals(">")
			|| queryOperator.equals(">="))
		{
			int r;
			if ((rawValue instanceof Number) || (queryValue instanceof Number))
			{
				Number num1;
				Number num2;
				if (rawValue instanceof Number)
					num1 = (Number) rawValue;
				else if (rawValue instanceof String)
				{
					num1 = Double.parseDouble((String) rawValue);
				}
				else
					return false;
				if (queryValue instanceof Number)
					num2 = (Number) queryValue;
				else if (queryValue instanceof String)
				{
					num2 = Double.parseDouble((String) queryValue);
				}
				else
					return false;

				if (num1.getClass().equals(num2.getClass()))
				{
					@SuppressWarnings("unchecked")
					Comparable<Number> c1 = (Comparable<Number>) num1;
					r = c1.compareTo(num2);
				}
				else
				{
					if ((num1 instanceof BigDecimal) || (num2 instanceof BigDecimal) || (num1 instanceof BigInteger)
						|| (num2 instanceof BigInteger))
					{
						BigDecimal bd1;
						BigDecimal bd2;
						if (num1 instanceof BigDecimal)
							bd1 = (BigDecimal) num1;
						else if (num1 instanceof BigInteger)
							bd1 = new BigDecimal((BigInteger) num1);
						else if (num1 instanceof Byte)
							bd1 = new BigDecimal(num1.byteValue());
						else if (num1 instanceof Double)
							bd1 = new BigDecimal(num1.doubleValue());
						else if (num1 instanceof Float)
							bd1 = new BigDecimal(num1.floatValue());
						else if (num1 instanceof Integer)
							bd1 = new BigDecimal(num1.intValue());
						else if (num1 instanceof Long)
							bd1 = new BigDecimal(num1.longValue());
						else if (num1 instanceof Short)
							bd1 = new BigDecimal(num1.shortValue());
						else
							throw new IllegalStateException();
						if (num2 instanceof BigDecimal)
							bd2 = (BigDecimal) num2;
						else if (num2 instanceof BigInteger)
							bd2 = new BigDecimal((BigInteger) num2);
						else if (num2 instanceof Byte)
							bd2 = new BigDecimal(num2.byteValue());
						else if (num2 instanceof Double)
							bd2 = new BigDecimal(num2.doubleValue());
						else if (num2 instanceof Float)
							bd2 = new BigDecimal(num2.floatValue());
						else if (num2 instanceof Integer)
							bd2 = new BigDecimal(num2.intValue());
						else if (num2 instanceof Long)
							bd2 = new BigDecimal(num2.longValue());
						else if (num2 instanceof Short)
							bd2 = new BigDecimal(num2.shortValue());
						else
							throw new IllegalStateException();
						r = bd1.compareTo(bd2);
					}
					else if ((num1 instanceof Float) || (num1 instanceof Double) || (num2 instanceof Float)
						|| (num2 instanceof Double))
					{
						Double d1;
						Double d2;
						if (num1 instanceof Double)
							d1 = (Double) num1;
						else
							d1 = new Double(num1.doubleValue());
						if (num2 instanceof Double)
							d2 = (Double) num2;
						else
							d2 = new Double(num2.doubleValue());
						r = d1.compareTo(d2);
					}
					else
					{
						Long l1;
						Long l2;
						if (num1 instanceof Long)
							l1 = (Long) num1;
						else
							l1 = new Long(num1.longValue());
						if (num2 instanceof Long)
							l2 = (Long) num2;
						else
							l2 = new Long(num2.longValue());
						r = l1.compareTo(l2);
					}
				}
			}
			else
			{
				String s1 = rawValue.toString();
				String s2 = queryValue.toString();
				r = s1.compareTo(s2);
			}
			if (queryOperator.equals("<"))
			{
				if (r >= 0)
					return false;
			}
			else if (queryOperator.equals("<="))
			{
				if (r > 0)
					return false;
			}
			else if (queryOperator.equals(">"))
			{
				if (r <= 0)
					return false;
			}
			else if (queryOperator.equals(">="))
			{
				if (r < 0)
					return false;
			}
		}
		return true;
	}

	protected <T> BaseOptions getStandardOptions(BaseQueryData<T> pQuery, int pLimit, Object pOffset)
	{
		QueryOptionCursorDetails cursorOpt = (QueryOptionCursorDetails) pQuery.option(QueryOptionCursorDetails.ID);
		QueryOptionOffset offsetOpt = (QueryOptionOffset) pQuery.option(QueryOptionOffset.ID);
		QueryOptionPage pageOpt = (QueryOptionPage) pQuery.option(QueryOptionPage.ID);
		QueryOptionState stateOpt = (QueryOptionState) pQuery.option(QueryOptionState.ID);

		/* Define the actual limit on the number of records to return */

		if (pageOpt.isPaginating())
			cursorOpt.cursorLimit = (pageOpt.pageSize == -1 ? Integer.MAX_VALUE : pageOpt.pageSize);
		else
		{
			if (pageOpt.isActive())
			{
				if ((pLimit != -1) && (pLimit != Integer.MAX_VALUE))
					cursorOpt.cursorLimit = pLimit;
				else
					cursorOpt.cursorLimit = (pageOpt.pageSize == -1 ? Integer.MAX_VALUE : pageOpt.pageSize);
			}
			else
				cursorOpt.cursorLimit = (pLimit == -1 ? Integer.MAX_VALUE : pLimit);
		}

		/* If the offset is provided then use it */

		if ((pOffset != null) && (pOffset instanceof Number) && (((Number) pOffset).intValue() != 0))
		{
			offsetOpt.activate();
			offsetOpt.offset = ((Number) pOffset).intValue();
		}

		/* Now calculate the offset */

		if (stateOpt.isStateless() || (stateOpt.isStateful() && cursorOpt.isActive() == false))
		{
			if (stateOpt.isStateless())
			{
				if (pageOpt.isPaginating())
				{
					if (offsetOpt.isActive())
					{
						cursorOpt.cursorOffset += offsetOpt.offset;
						offsetOpt.passivate();
					}
				}
				else
				{
					if (pageOpt.isActive())
						pageOpt.passivate();
					if (offsetOpt.isActive())
					{
						cursorOpt.cursorOffset = offsetOpt.offset;
						offsetOpt.passivate();
					}
					else
						cursorOpt.cursorOffset = 0;
				}
			}
			else
			{
				if (offsetOpt.isActive())
				{
					cursorOpt.cursorOffset += offsetOpt.offset;
					offsetOpt.passivate();
				}
			}
		}
		else
		{
			if (offsetOpt.isActive())
			{
				cursorOpt.cursorOffset += offsetOpt.offset;
				offsetOpt.passivate();
			}
		}

		return new BaseOptions(pageOpt, stateOpt, cursorOpt);
	}

	@SuppressWarnings("unchecked")
	protected String convertToString(EXTCLASSINFO pClassInfo, Field pField, Class<?> pClass, Object pValue)
	{
		String result;
		if (pValue == null)
			result = null;
		else if ((pClass == Boolean.class) || (pClass == Boolean.TYPE))
			result = pValue.toString();
		else if ((pClass == Byte.class) || (pClass == Byte.TYPE))
			result = pValue.toString();
		else if ((pClass == Character.class) || (pClass == Character.TYPE))
			result = pValue.toString();
		else if ((pClass == Double.class) || (pClass == Double.TYPE))
			result = pValue.toString();
		else if ((pClass == Float.class) || (pClass == Float.TYPE))
			result = pValue.toString();
		else if ((pClass == Integer.class) || (pClass == Integer.TYPE))
			result = pValue.toString();
		else if ((pClass == Long.class) || (pClass == Long.TYPE))
			result = pValue.toString();
		else if ((pClass == Short.class) || (pClass == Short.TYPE))
			result = pValue.toString();
		else if (pClass == String.class)
			result = (String) pValue;
		else if (Enum.class.isAssignableFrom(pClass))
			result = pValue.toString();
		else if (pClass == BigDecimal.class)
			result = pValue.toString();
		else if (Date.class.isAssignableFrom(pClass))
		{
			if (pField != null)
				result = Util.toString(pField, pValue);
			else
				result = Util.timestamp((Date) pValue);
		}
		else if (Json.class.isAssignableFrom(pClass))
			result = pValue.toString();
		else if ((pField != null) && (pField.getAnnotation(Embedded.class) != null))
		{
			Embedded embedded = pField.getAnnotation(Embedded.class);
			switch (embedded.mode())
			{
			case NATIVE:
				result = nativeSerialize(pValue);
				break;
			case SERIALIZE_JSON:
				Json json = JsonSerializer.serialize(pValue, pField);
				result = json.toString();
				break;
			case SERIALIZE_JAVA:
				ByteArrayOutputStream bos = new ByteArrayOutputStream();
				ObjectOutput out;
				try
				{
					out = new ObjectOutputStream(bos);
					out.writeObject(pValue);
					out.close();
				}
				catch (IOException e)
				{
					throw new SienaException(e);
				}

				result = new String(Base64.encodeBase64(bos.toByteArray()));
				break;
			default:
				throw new IllegalArgumentException("Unreccognized Embedded type");
			}
		}
		else if (pClass == byte[].class)
			result = new String(Base64.encodeBase64((byte[]) pValue));
		else if ((pField != null) && (pField.getAnnotation(Polymorphic.class) != null))
		{
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			ObjectOutput out;
			try
			{
				out = new ObjectOutputStream(bos);
				out.writeObject(pValue);
				out.close();
			}
			catch (IOException e)
			{
				throw new SienaException(e);
			}

			result = new String(Base64.encodeBase64(bos.toByteArray()));
		}
		else if (Collection.class.isAssignableFrom(pClass))
		{
			StringBuilder sb = new StringBuilder();
			boolean isFirst = true;
			try
			{
				@SuppressWarnings("rawtypes")
				Collection col = (Collection) pValue;
				for (Object obj : col)
				{
					if (isFirst == true)
						isFirst = false;
					else
						sb.append('&');
					Class<?> valueClass = obj.getClass();
					EXTCLASSINFO newClassInfo = getClassInfo(valueClass, true);
					sb.append(mStringEncoder.encode(valueClass.getName()));
					sb.append('=');
					sb.append(mStringEncoder.encode(convertToString(newClassInfo, pField, valueClass, obj)));
				}
			}
			catch (EncoderException ex)
			{
				throw new RuntimeException(ex);
			}
			result = sb.toString();
		}
		else
		{
			if (ClassInfo.isModel(pClass))
			{
				EXTCLASSINFO newClassInfo = getClassInfo(pClass, true);

				pValue = getKeysOfObject(newClassInfo, pValue);
				StringBuilder sb = new StringBuilder();
				boolean first = false;
				for (Map.Entry<String, Object> pair : ((Map<String, Object>) pValue).entrySet())
				{
					String key = pair.getKey();
					Field field = newClassInfo.mColumnNameToFieldMap.get(key);
					Object value = pair.getValue();
					Class<?> valueClass = value.getClass();
					String valueStr = convertToString(newClassInfo, field, valueClass, value);
					try
					{
						if (first == false)
							first = true;
						else
							sb.append('&');
						sb.append(mStringEncoder.encode(key));
						sb.append("=");
						/*
						 * This is a short cut. Since the majority of key-value stores are just using strings, don't
						 * expand the storage by indicating that it is a string
						 */
						if (valueClass != String.class)
						{
							sb.append(mStringEncoder.encode(valueClass.getName()));
							sb.append(':');
						}
						sb.append(mStringEncoder.encode(valueStr));
					}
					catch (EncoderException ex)
					{
						throw new RuntimeException(ex);
					}
				}
				result = sb.toString();
			}
			else
			{
				throw new IllegalArgumentException("Unable to convert a " + pClass.getName() + " to a string.");
			}
		}
		return result;
	}

	@SuppressWarnings("unchecked")
	protected Object convertToPrimitive(EXTCLASSINFO pClassInfo, Field pField, Class<?> pClass, Object pValue)
	{
		Object result;
		if (pValue == null)
			result = null;
		else if ((pClass == Boolean.class) || (pClass == Boolean.TYPE))
			result = pValue;
		else if ((pClass == Byte.class) || (pClass == Byte.TYPE))
			result = pValue;
		else if ((pClass == Character.class) || (pClass == Character.TYPE))
			result = pValue;
		else if ((pClass == Double.class) || (pClass == Double.TYPE))
			result = pValue;
		else if ((pClass == Float.class) || (pClass == Float.TYPE))
			result = pValue;
		else if ((pClass == Integer.class) || (pClass == Integer.TYPE))
			result = pValue;
		else if ((pClass == Long.class) || (pClass == Long.TYPE))
			result = pValue;
		else if ((pClass == Short.class) || (pClass == Short.TYPE))
			result = pValue;
		else if (pClass == String.class)
			result = pValue;
		else if (Enum.class.isAssignableFrom(pClass))
			result = pValue.toString();
		else if (pClass == BigDecimal.class)
			result = pValue;
		else if (Date.class.isAssignableFrom(pClass))
			result = pValue;
		else if (Json.class.isAssignableFrom(pClass))
			result = pValue.toString();
		else if (pField.getAnnotation(Embedded.class) != null)
		{
			Embedded embedded = pField.getAnnotation(Embedded.class);
			switch (embedded.mode())
			{
			case NATIVE:
				result = nativeSerialize(pValue);
				break;
			case SERIALIZE_JSON:
				Json json = JsonSerializer.serialize(pValue, pField);
				result = json.toString();
				break;
			case SERIALIZE_JAVA:
				ByteArrayOutputStream bos = new ByteArrayOutputStream();
				ObjectOutput out;
				try
				{
					out = new ObjectOutputStream(bos);
					out.writeObject(pValue);
					out.close();
				}
				catch (IOException e)
				{
					throw new SienaException(e);
				}

				result = new String(Base64.encodeBase64(bos.toByteArray()));
				break;
			default:
				throw new IllegalArgumentException("Unreccognized Embedded type");
			}
		}
		else if (pClass == byte[].class)
			result = pValue;
		else if (pField.getAnnotation(Polymorphic.class) != null)
		{
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			ObjectOutput out;
			try
			{
				out = new ObjectOutputStream(bos);
				out.writeObject(pValue);
				out.close();
			}
			catch (IOException e)
			{
				throw new SienaException(e);
			}

			result = new String(Base64.encodeBase64(bos.toByteArray()));
		}
		else
		{
			if (ClassInfo.isModel(pClass))
			{
				EXTCLASSINFO newClassInfo = getClassInfo(pClass, true);

				pValue = getKeysOfObject(newClassInfo, pValue);
				StringBuilder sb = new StringBuilder();
				boolean first = false;
				for (Map.Entry<String, Object> pair : ((Map<String, Object>) pValue).entrySet())
				{
					String key = pair.getKey();
					Field field = newClassInfo.mColumnNameToFieldMap.get(key);
					Object value = pair.getValue();
					Class<?> valueClass = value.getClass();
					String valueStr = convertToString(newClassInfo, field, valueClass, value);
					try
					{
						if (first == false)
							first = true;
						else
							sb.append('&');
						sb.append(mStringEncoder.encode(key));
						sb.append("=");
						/*
						 * This is a short cut. Since the majority of key-value stores are just using strings, don't
						 * expand the storage by indicating that it is a string
						 */
						if (valueClass != String.class)
						{
							sb.append(mStringEncoder.encode(valueClass.getName()));
							sb.append(':');
						}
						sb.append(mStringEncoder.encode(valueStr));
					}
					catch (EncoderException ex)
					{
						throw new RuntimeException(ex);
					}
				}
				result = sb.toString();
			}
			else
			{
				throw new IllegalArgumentException("Unable to convert a " + pClass.getName() + " to a string.");
			}
		}
		return result;
	}

	/**
	 * Converts a string into a given type
	 * 
	 * @param pClass the class requested
	 * @param pValue the value
	 * @return the value in the class requested
	 */
	@SuppressWarnings("unchecked")
	protected <T> T convertFromString(EXTCLASSINFO pCLInfo, Field pField, Class<T> pClass, String pValue)
	{
		T result;
		if (pValue == null)
		{
			if (pClass.isPrimitive())
			{
				if (pClass == Boolean.TYPE)
					result = (T) Boolean.FALSE;
				else if (pClass == Byte.TYPE)
					result = (T) new Byte(Byte.MIN_VALUE);
				else if (pClass == Character.TYPE)
					result = (T) new Character(Character.MIN_VALUE);
				else if (pClass == Double.TYPE)
					result = (T) new Double(0d);
				else if (pClass == Float.TYPE)
					result = (T) new Float(0f);
				else if (pClass == Integer.TYPE)
					result = (T) new Integer(0);
				else if (pClass == Long.TYPE)
					result = (T) new Long(0L);
				else if (pClass == Short.TYPE)
					result = (T) new Short((short) 0);
				else
					throw new IllegalArgumentException("Unrecognized primitive type: " + pClass.getName());
			}
			else
				result = null;
		}
		else if ((pClass == Boolean.class) || (pClass == Boolean.TYPE))
			result = (T) new Boolean(Boolean.parseBoolean(pValue));
		else if ((pClass == Byte.class) || (pClass == Byte.TYPE))
			result = (T) new Byte(Byte.parseByte(pValue));
		else if ((pClass == Character.class) || (pClass == Character.TYPE))
			result = (T) new Character(pValue.charAt(0));
		else if ((pClass == Double.class) || (pClass == Double.TYPE))
			result = (T) new Double(Double.parseDouble(pValue));
		else if ((pClass == Float.class) || (pClass == Float.TYPE))
			result = (T) new Float(Float.parseFloat(pValue));
		else if ((pClass == Integer.class) || (pClass == Integer.TYPE))
			result = (T) new Integer(Integer.parseInt(pValue));
		else if ((pClass == Long.class) || (pClass == Long.TYPE))
			result = (T) new Long(Long.parseLong(pValue));
		else if ((pClass == Short.class) || (pClass == Short.TYPE))
			result = (T) new Short(Short.parseShort(pValue));
		else if (pClass == String.class)
			result = (T) pValue;
		else if (Enum.class.isAssignableFrom(pClass))
		{
			@SuppressWarnings("rawtypes")
			Class c = pClass;
			Enum<?> valueOf = Enum.valueOf(c, pValue);
			result = (T) valueOf;
		}
		else if (pClass == BigDecimal.class)
			result = (T) new BigDecimal(pValue);
		else if (Date.class.isAssignableFrom(pClass))
		{
			if (pField != null)
			{
				if (pField.getAnnotation(DateTime.class) != null)
					result = (T) Util.timestamp(pValue);
				else if (pField.getAnnotation(Time.class) != null)
					result = (T) Util.time(pValue);
				else if (pField.getAnnotation(SimpleDate.class) != null)
					result = (T) Util.date(pValue);
				else
					result = (T) Util.timestamp(pValue);
			}
			else
				result = (T) Util.timestamp(pValue);
		}
		else if (Json.class.isAssignableFrom(pClass))
			result = (T) Json.loads(pValue);
		else if (pField.getAnnotation(Embedded.class) != null)
		{
			Embedded embedded = pField.getAnnotation(Embedded.class);
			switch (embedded.mode())
			{
			case NATIVE:
				result = (T) nativeDeserialize(pValue);
				break;
			case SERIALIZE_JSON:
				Json json = Json.loads(pValue);
				result = (T) JsonSerializer.deserialize(pField, json);
				break;
			case SERIALIZE_JAVA:
				byte[] data = Base64.decodeBase64(pValue.getBytes());
				ByteArrayInputStream bais = new ByteArrayInputStream(data);
				ObjectInputStream ois = null;
				try
				{
					ois = new ObjectInputStream(bais);
					result = (T) ois.readObject();
				}
				catch (IOException ex)
				{
					throw new SienaException(ex);
				}
				catch (ClassNotFoundException ex)
				{
					throw new SienaException(ex);
				}
				finally
				{
					try
					{
						ois.close();
					}
					catch (IOException ex)
					{
						throw new RuntimeException(ex);
					}
				}
				break;
			default:
				throw new IllegalArgumentException("Unrecognized embedded mode");
			}
		}
		else if (pClass == byte[].class)
			result = (T) Base64.decodeBase64(pValue.getBytes());
		else if (pField.getAnnotation(Polymorphic.class) != null)
		{
			byte[] data = Base64.decodeBase64(pValue.getBytes());
			ByteArrayInputStream bais = new ByteArrayInputStream(data);
			ObjectInputStream ois = null;
			try
			{
				ois = new ObjectInputStream(bais);
				result = (T) ois.readObject();
			}
			catch (IOException ex)
			{
				throw new SienaException(ex);
			}
			catch (ClassNotFoundException ex)
			{
				throw new SienaException(ex);
			}
			finally
			{
				try
				{
					ois.close();
				}
				catch (IOException ex)
				{
					throw new RuntimeException(ex);
				}
			}
		}
		else if (Collection.class.isAssignableFrom(pClass))
		{
			Collection<Object> col;
			if (List.class.isAssignableFrom(pClass))
				col = new ArrayList<Object>();
			else if (Queue.class.isAssignableFrom(pClass))
				col = new LinkedList<Object>();
			else if (Set.class.isAssignableFrom(pClass))
				col = new HashSet<Object>();
			else
				throw new IllegalArgumentException("Unsupported collection type");
			try
			{
				for (StringTokenizer st = new StringTokenizer(pValue, "&"); st.hasMoreTokens();)
				{
					String token = st.nextToken();
					int offset = token.indexOf('=');
					String className = mStringEncoder.decode(token.substring(0, offset));
					String data = mStringEncoder.decode(token.substring(offset + 1));
					Class<?> valueClass;
					try
					{
						valueClass = Class.forName(className);
					}
					catch (ClassNotFoundException ex)
					{
						throw new RuntimeException(ex);
					}
					EXTCLASSINFO newClassInfo = getClassInfo(valueClass, true);
					Object r = convertFromString(newClassInfo, pField, valueClass, data);
					col.add(r);
				}
			}
			catch (DecoderException ex)
			{
				throw new RuntimeException(ex);
			}
			result = (T) col;
		}
		else
		{
			if (ClassInfo.isModel(pClass))
			{
				/* It's a persisted map */

				Map<String, Object> pmap = new HashMap<String, Object>();
				for (Enumeration<Object> e = new StringTokenizer(pValue, "&", false); e.hasMoreElements();)
				{
					String token = (String) e.nextElement();
					int offset = token.indexOf('=');
					int valName = token.indexOf(':', offset + 1);
					String key = token.substring(0, offset);
					Class<?> vClass;
					String value;
					if (valName == -1)
					{
						vClass = String.class;
						value = token.substring(offset + 1);
					}
					else
					{
						String className = token.substring(offset + 1, valName);
						try
						{
							vClass = Class.forName(className);
						}
						catch (ClassNotFoundException ex)
						{
							throw new RuntimeException(ex);
						}
						value = token.substring(valName + 1);
					}

					try
					{
						Object realVal = convertFromString(null, null, vClass, mStringEncoder.decode(value));
						pmap.put(mStringEncoder.decode(key), realVal);
					}
					catch (DecoderException ex)
					{
						throw new RuntimeException(ex);
					}
				}
				result = (T) pmap;
			}
			else
			{
				/* fromString, valueOf, parseXXX */
				Method d = mCachedMethods.get(pClass);
				if (d == null)
				{
					try
					{
						try
						{
							d = pClass.getMethod("fromString", new Class<?>[] {String.class});
						}
						catch (NoSuchMethodException ex)
						{
							try
							{
								d = pClass.getMethod("valueOf", new Class<?>[] {String.class});
							}
							catch (NoSuchMethodException ex1)
							{
								try
								{
									d = pClass.getMethod("parse" + pClass.getName(), new Class<?>[] {String.class});
								}
								catch (NoSuchMethodException ex2)
								{
									throw new IllegalArgumentException("Unrecognized type: " + pClass.getName());
								}
							}
						}
					}
					catch (SecurityException ex)
					{
						throw new RuntimeException(ex);
					}
					mCachedMethods.put(pClass, d);
				}
				try
				{
					result = (T) d.invoke(null, new Object[] {pValue});
				}
				catch (IllegalAccessException ex)
				{
					throw new RuntimeException(ex);
				}
				catch (IllegalArgumentException ex)
				{
					throw new RuntimeException(ex);
				}
				catch (InvocationTargetException ex)
				{
					throw new RuntimeException(ex);
				}
			}
		}
		return result;
	}

	/**
	 * Adds a new callback to be run when the transaction is complete
	 * 
	 * @param pTransScope the scope
	 * @param pFunction the function
	 * @param pData the data to pass to the function
	 */
	protected <F> void addCallbackAfterSuccessfulTransaction(EXTTRANSSCOPE pTransScope, Function<F, F> pFunction,
		F pData)
	{
		EXTTRANSSCOPE rootScope = getRootTransScope(pTransScope);
		if (rootScope.afterSuccess == null)
			rootScope.afterSuccess = new ArrayList<ExtTransScope.CallData<Object>>();
		@SuppressWarnings("unchecked")
		CallData<Object> callData = (CallData<Object>) new CallData<F>(pFunction, pData);
		rootScope.afterSuccess.add(callData);
	}

	/**
	 * Performs a native serialization. In this case, it just converts all the parameters into x=y& string
	 * 
	 * @param pValue
	 * @return
	 */
	public String nativeSerialize(Object pValue)
	{
		if (pValue == null)
			return null;
		Class<?> clazz = pValue.getClass();
		EXTCLASSINFO clInfo = getClassInfo(clazz, true);
		List<MapData<EXTLOWOBJECT>> map = convertToMap(clInfo, pValue, false);
		StringBuilder sb = new StringBuilder();
		boolean isFirst = true;
		try
		{
			for (MapData<EXTLOWOBJECT> md : map)
			{
				if (isFirst)
					isFirst = false;
				else
					sb.append('&');
				StringBuilder sb2 = new StringBuilder();
				sb2.append("name=");
				sb2.append(mStringEncoder.encode(md.name));
				sb2.append("&type=");
				sb2.append(mStringEncoder.encode(md.type));
				sb2.append("&classname=");
				sb2.append(mStringEncoder.encode(md.className));
				sb2.append("&data=");
				sb2.append(mStringEncoder.encode(convertLowToString(md.data)));
				sb.append(mStringEncoder.encode(sb2.toString()));
			}
			return sb.toString();
		}
		catch (EncoderException ex)
		{
			throw new RuntimeException(ex);
		}
	}

	/**
	 * This performs the deserialization of an object that was previously serialized with the nativeSerialize method.
	 * 
	 * @param pValue the string
	 * @return the object
	 */
	public Object nativeDeserialize(String pValue)
	{
		if (pValue == null)
			return null;
		List<MapData<EXTLOWOBJECT>> mapList = new ArrayList<MapData<EXTLOWOBJECT>>();
		try
		{
			for (StringTokenizer st = new StringTokenizer(pValue, "&"); st.hasMoreTokens();)
			{
				String token = mStringEncoder.decode(st.nextToken());
				MapData<EXTLOWOBJECT> map = new MapData<EXTLOWOBJECT>();
				String dataStr = null;
				for (StringTokenizer st2 = new StringTokenizer(token, "&"); st2.hasMoreTokens();)
				{
					String token2 = st2.nextToken();
					int offset = token2.indexOf('=');
					String key = mStringEncoder.decode(token2.substring(0, offset));
					String value = mStringEncoder.decode(token2.substring(offset + 1));
					if ("name".equals(key))
						map.name = value;
					else if ("type".equals(key))
						map.type = value;
					else if ("classname".equals(key))
						map.className = value;
					else if ("data".equals(key))
						dataStr = value;
				}
				map.data = convertStringToLow(dataStr);
				mapList.add(map);
			}
		}
		catch (DecoderException ex)
		{
			throw new RuntimeException(ex);
		}

		/* Now convert the map list back into an object */

		if (mapList.size() != 1)
			throw new IllegalArgumentException("Not supporting multiple native objects");

		MapData<EXTLOWOBJECT> map = mapList.get(0);
		Class<?> clazz;
		try
		{
			clazz = Class.forName(map.className);
		}
		catch (ClassNotFoundException ex)
		{
			throw new RuntimeException(ex);
		}
		EXTCLASSINFO clInfo = getClassInfo(clazz, true);
		Function<EXTLOWOBJECT, Object> f = getFunctionFromLowToHigh(clInfo, false);
		return f.apply(map.data);
	}

	/* ********************************************************************************
	 * ABSTRACT
	 */

	/**
	 * Returns a ClassInfo for a given class. The class info contains all sorts of pre-calculated data to make it faster
	 * to process the data. The class info object returned should be a singleton (ie. it can be used as a key) for a
	 * given class.
	 * 
	 * @param pClass the class
	 * @param pCreateIfMissing true if the class info should be created or false if it should just return null if the
	 *            the class hasn't been previously processed
	 * @return the new class info
	 */
	protected abstract EXTCLASSINFO getClassInfo(Class<?> pClass, boolean pCreateIfMissing);

	/**
	 * Open a new transaction scope. If a transaction is already running on the given thread, then this transaction is
	 * used instead of creating a new one. The returned EXTTRANSSCOPE object contains a flag keepOpen that can be set to
	 * true or false by code. Generally, it should be untouched, but manually setting it to 'false' can force a
	 * transaction to commit/rollback during the next closeTransactionScope call (usually only executed by a
	 * commitTransaction or rollbackTransaction method.
	 * 
	 * @param pImplicitTransaction true if this is an implicit transaction or false if this represents a
	 *            'beginTransaction'
	 * @return the new scope
	 */
	protected abstract EXTTRANSSCOPE openTransactionScope(boolean pImplicitTransaction);

	/**
	 * Close a transaction scope. The result type (stored in the scope) indicates how the transaction should be handled
	 * if the transaction is actually being closed. COMMIT indicates that all the data should be permanently committed.
	 * ROLLBACK indicates that the data should be rolled back and READONLY indicates that there were no inserts, updates
	 * or deletes.
	 * 
	 * @param pScope the scope previously opened by openTransactionScope
	 */
	protected abstract void closeTransactionScope(EXTTRANSSCOPE pScope);

	/**
	 * Returns the root transaction code. This is used when there are nested scopes.
	 * 
	 * @param pTransScope the scope to find the root
	 * @return the root scope
	 */
	protected abstract EXTTRANSSCOPE getRootTransScope(EXTTRANSSCOPE pTransScope);

	/**
	 * This creates an empty, low level object.
	 * 
	 * @param pCLInfo the class info for the object to create.
	 * @return the new low level object
	 */
	protected abstract EXTLOWOBJECT createLow(EXTCLASSINFO pCLInfo);

	/**
	 * Returns a value from the low level object
	 * 
	 * @param pObj the low level object
	 * @param pCLInfo the class info for the object
	 * @param pResultClass the class of the resulting object. This must be a 'conversion' class.
	 * @param pField the field from the high level object to retrieve (ie. internally it needs to look up the low level
	 *            column name)
	 * @return the value
	 */
	protected abstract <T> T getLowColumnValue(EXTLOWOBJECT pObj, EXTCLASSINFO pCLInfo, Class<T> pResultClass,
		Field pField);

	/**
	 * Returns a value from the low level object.
	 * 
	 * @param pObj the low level object
	 * @param pCLInfo the class info for the object
	 * @param pResultClass the class of the resulting object. This must be a 'conversion' class.
	 * @param pColumnName the low level column name
	 * @return the value
	 */
	protected abstract <T> T getLowColumnValue(EXTLOWOBJECT pObj, EXTCLASSINFO pCLInfo, Class<T> pResultClass,
		String pColumnName);

	/**
	 * This returns the raw low column value without actually storing it in the object. This is primarily used with the
	 * compareLowColumnValue.
	 * 
	 * @param pCLInfo the class info for the object
	 * @param pField the field from the high level object
	 * @param pValue the high level value
	 * @return the raw low level value
	 */
	protected abstract Object convertToRawLowColumnValue(EXTCLASSINFO pCLInfo, Field pField, Object pValue);

	/**
	 * This converts a low object into a string. Mostly used for serialization, and will be reconstituted using the
	 * convertStringToLow.
	 * 
	 * @param pObj the low object
	 * @return the string
	 */
	protected abstract String convertLowToString(EXTLOWOBJECT pObj);

	/**
	 * This converts a string back into a low object
	 * 
	 * @param pValue the string
	 * @return the low object
	 */
	protected abstract EXTLOWOBJECT convertStringToLow(String pValue);

	/**
	 * This compares the given raw low level value with the value stored in a low level object.
	 * 
	 * @param pObj the low level object
	 * @param pCLInfo the class info for the object
	 * @param pField the field from the high level object
	 * @param pRawValue the raw low level value to compare against
	 * @return negative integer if it's less than the object, 0 if it's the same, and a positive integer if it's greater
	 *         than the object
	 */
	protected abstract int compareRawToLowColumnValue(EXTLOWOBJECT pObj, EXTCLASSINFO pCLInfo, Field pField,
		Object pRawValue);

	/**
	 * Stores a new value in the low level object. The value being stored should either be a "conversion" value.
	 * "conversion" values are those than can be converted to and from a String using the
	 * convertToString/convertFromString
	 * 
	 * @param pObj the low level representation object
	 * @param pCLInfo the class info for the object
	 * @param pField the field from the high level object to store
	 * @param pConversionValue the value to store.
	 */
	protected abstract void setLowColumnValue(EXTLOWOBJECT pObj, EXTCLASSINFO pCLInfo, Field pField,
		Object pConversionValue);

	/**
	 * This retrieves a complete raw object from a given key raw object. The key object just contains the key values
	 * 
	 * @param pCLInfo the class of the object
	 * @param pKeyObj the key object
	 * @return the complete low level representation object. Must return NULL if the object does not exist.
	 */
	protected abstract EXTLOWOBJECT loadLowFromLowKeys(EXTCLASSINFO pCLInfo, EXTLOWOBJECT pKeyObj);

	/**
	 * This returns all the 'column names' within the given low level representation object. The low level object can be
	 * either a complete object or a keys only object.
	 * 
	 * @param pObj the low level object
	 * @return the list of column names
	 */
	protected abstract Set<String> getLowColumnNames(EXTLOWOBJECT pObj);

	/**
	 * This returns a function that can be used to convert a low level object into it's corresponding high level object.
	 * 
	 * @param pCLInfo the class info of the object
	 * @param pOnlyKeys true if the resulting object should only contains the keys or false if it should contain the
	 *            entire object
	 * @return the function
	 */
	protected abstract <T> Function<EXTLOWOBJECT, T> getFunctionFromLowToHigh(EXTCLASSINFO pCLInfo, boolean pOnlyKeys);

	/**
	 * This returns a function that can be used to convert a low level object to a low level object with just the keys
	 * 
	 * @param pCLInfo the class info of the object
	 * @return the low level key only object
	 */
	protected abstract Function<EXTLOWOBJECT, EXTLOWOBJECT> getFunctionFromLowToLowKey(EXTCLASSINFO pCLInfo);

	/**
	 * Generate a new UUID. There are many ways to generate this key, so it's been delegated to the actual
	 * implementation.
	 * 
	 * @param pUUIDClass the type of UUID. The only supported types are String and java.util.UUID
	 * @return the UUID
	 */
	protected abstract <T> T generateUUID(Class<T> pUUIDClass);

	/**
	 * Generates a new sequence number for the given table/column
	 * 
	 * @param pCLInfo the class of the object
	 * @param pColumnName the column of the object
	 * @return the next sequence number
	 */
	protected abstract long generateNextSequence(EXTCLASSINFO pCLInfo, String pColumnName);

	/**
	 * Construct the additional query info
	 * 
	 * @return
	 */
	protected abstract <T> EXTQUERYINFO createQueryInfo(EXTCLASSINFO pCLInfo, BaseQueryData<T> pQuery, boolean pKeysOnly);

	/**
	 * This deletes an object given a key in a low level object
	 * 
	 * @param pCLInfo the class info of the object
	 * @param pLowKey the low level key object
	 */
	protected abstract SienaFuture<Boolean> deleteByLowKey(EXTCLASSINFO pCLInfo, EXTLOWOBJECT pLowKey);

	protected abstract <T, O> Iterator<O> retrieveData(EXTTRANSSCOPE pScope, EXTCLASSINFO pCLInfo,
		BaseQueryData<T> pQuery, EXTQUERYINFO pQueryInfo, int pLimit, Object pOffset,
		Function<EXTLOWOBJECT, O> pTransformers);

	protected abstract void storeMapData(EXTTRANSSCOPE pScope, EXTCLASSINFO pClInfo, List<MapData<EXTLOWOBJECT>> pData,
		boolean pIsInsert);

	/**
	 * Returns whether this persistence manager is able to support both a set and a get operation on the same object in
	 * a single transaction. Alot of key-value stores don't support this, since you have to complete the transaction
	 * before you get the results of the get back. This is also used to handle sequence number generation for inserts.
	 * If get/set is not supported, then a failure in an insert transaction still causes the sequence number to be
	 * consumed
	 * 
	 * @return true if gets and sets on the same object is supported in a single transaction or false otherwise
	 */
	protected abstract boolean supportsGetSetTransactions();
}