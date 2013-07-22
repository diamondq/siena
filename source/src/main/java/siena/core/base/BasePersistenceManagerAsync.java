package siena.core.base;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import siena.BaseQueryData;
import siena.PersistenceManager;
import siena.core.async.AbstractPersistenceManagerAsync;
import siena.core.async.QueryAsync;
import siena.core.async.SienaFuture;
import siena.core.async.SienaFutureMock;

public class BasePersistenceManagerAsync<EXTCLASSINFO extends ExtClassInfo, EXTQUERYINFO extends BaseQueryInfo<?, EXTCLASSINFO>, EXTTRANSSCOPE extends ExtTransScope, EXTLOWOBJECT>
	extends AbstractPersistenceManagerAsync
{

	protected BasePersistenceManager<EXTCLASSINFO, EXTQUERYINFO, EXTTRANSSCOPE, EXTLOWOBJECT>	mSyncPM;

	public BasePersistenceManagerAsync(
		BasePersistenceManager<EXTCLASSINFO, EXTQUERYINFO, EXTTRANSSCOPE, EXTLOWOBJECT> pSyncPM)
	{
		mSyncPM = pSyncPM;
	}

	/**
	 * @see siena.core.async.PersistenceManagerAsync#init(java.util.Properties)
	 */
	@Override
	public void init(Properties pP)
	{
	}

	/**
	 * @see siena.core.async.AbstractPersistenceManagerAsync#createQuery(java.lang.Class)
	 */
	@Override
	public <T> QueryAsync<T> createQuery(Class<T> clazz)
	{
		return new ExtBaseQueryAsync<T>(this, clazz);
	}

	/**
	 * @see siena.core.async.AbstractPersistenceManagerAsync#createQuery(siena.BaseQueryData)
	 */
	@Override
	public <T> QueryAsync<T> createQuery(BaseQueryData<T> data)
	{
		return new ExtBaseQueryAsync<T>(this, data);
	}

	/**
	 * @see siena.core.async.PersistenceManagerAsync#sync()
	 */
	@Override
	public PersistenceManager sync()
	{
		return mSyncPM;
	}

	/**
	 * @see siena.core.async.PersistenceManagerAsync#get(java.lang.Object)
	 */
	@Override
	public SienaFuture<Void> get(Object pObj)
	{
		mSyncPM.get(pObj);
		return ConstantSienaFuture.sVOID;
	}

	/**
	 * @see siena.core.async.PersistenceManagerAsync#insert(java.lang.Object[])
	 */
	@Override
	public SienaFuture<Integer> insert(Object... pObjects)
	{
		return new SienaFutureMock<Integer>(mSyncPM.insert(pObjects));
	}

	/**
	 * @see siena.core.async.PersistenceManagerAsync#insert(java.lang.Iterable)
	 */
	@Override
	public SienaFuture<Integer> insert(Iterable<?> pObjects)
	{
		return new SienaFutureMock<Integer>(mSyncPM.insert(pObjects));
	}

	/**
	 * @see siena.core.async.PersistenceManagerAsync#insert(java.lang.Object)
	 */
	@Override
	public SienaFuture<Void> insert(Object pObj)
	{
		mSyncPM.insert(pObj);
		return ConstantSienaFuture.sVOID;
	}

	/**
	 * @see siena.core.async.PersistenceManagerAsync#delete(java.lang.Object[])
	 */
	@Override
	public SienaFuture<Integer> delete(Object... pModels)
	{
		return new SienaFutureMock<Integer>(mSyncPM.delete(pModels));
	}

	/**
	 * @see siena.core.async.PersistenceManagerAsync#delete(java.lang.Iterable)
	 */
	@Override
	public SienaFuture<Integer> delete(Iterable<?> pModels)
	{
		return new SienaFutureMock<Integer>(mSyncPM.delete(pModels));
	}

	/**
	 * @see siena.core.async.PersistenceManagerAsync#deleteByKeys(java.lang.Class, java.lang.Object[])
	 */
	@Override
	public <T> SienaFuture<Integer> deleteByKeys(Class<T> pClazz, Object... pKeys)
	{
		return new SienaFutureMock<Integer>(mSyncPM.deleteByKeys(pClazz, pKeys));
	}

	/**
	 * @see siena.core.async.PersistenceManagerAsync#deleteByKeys(java.lang.Class, java.lang.Iterable)
	 */
	@Override
	public <T> SienaFuture<Integer> deleteByKeys(Class<T> pClazz, Iterable<?> pKeys)
	{
		return new SienaFutureMock<Integer>(mSyncPM.deleteByKeys(pClazz, pKeys));
	}

	/**
	 * @see siena.core.async.PersistenceManagerAsync#delete(java.lang.Object)
	 */
	@Override
	public SienaFuture<Void> delete(Object pObj)
	{
		mSyncPM.delete(pObj);
		return ConstantSienaFuture.sVOID;
	}

	/**
	 * @see siena.core.async.PersistenceManagerAsync#update(java.lang.Object)
	 */
	@Override
	public SienaFuture<Void> update(Object pObj)
	{
		mSyncPM.update(pObj);
		return ConstantSienaFuture.sVOID;
	}

	/**
	 * @see siena.core.async.PersistenceManagerAsync#save(java.lang.Object)
	 */
	@Override
	public SienaFuture<Void> save(Object pObj)
	{
		mSyncPM.save(pObj);
		return ConstantSienaFuture.sVOID;
	}

	/**
	 * @see siena.core.async.PersistenceManagerAsync#save(java.lang.Object[])
	 */
	@Override
	public SienaFuture<Integer> save(Object... pObjects)
	{
		return new SienaFutureMock<Integer>(mSyncPM.save(pObjects));
	}

	/**
	 * @see siena.core.async.PersistenceManagerAsync#save(java.lang.Iterable)
	 */
	@Override
	public SienaFuture<Integer> save(Iterable<?> pObjects)
	{
		return new SienaFutureMock<Integer>(mSyncPM.save(pObjects));
	}

	/**
	 * @see siena.core.async.PersistenceManagerAsync#get(java.lang.Object[])
	 */
	@Override
	public SienaFuture<Integer> get(Object... pModels)
	{
		return new SienaFutureMock<Integer>(mSyncPM.get(pModels));
	}

	/**
	 * @see siena.core.async.PersistenceManagerAsync#get(java.lang.Iterable)
	 */
	@Override
	public <T> SienaFuture<Integer> get(Iterable<T> pModels)
	{
		return new SienaFutureMock<Integer>(mSyncPM.get(pModels));
	}

	/**
	 * @see siena.core.async.PersistenceManagerAsync#getByKey(java.lang.Class, java.lang.Object)
	 */
	@Override
	public <T> SienaFuture<T> getByKey(Class<T> pClazz, Object pKey)
	{
		return new SienaFutureMock<T>(mSyncPM.getByKey(pClazz, pKey));
	}

	/**
	 * @see siena.core.async.PersistenceManagerAsync#getByKeys(java.lang.Class, java.lang.Object[])
	 */
	@Override
	public <T> SienaFuture<List<T>> getByKeys(Class<T> pClazz, Object... pKeys)
	{
		return new SienaFutureMock<List<T>>(mSyncPM.getByKeys(pClazz, pKeys));
	}

	/**
	 * @see siena.core.async.PersistenceManagerAsync#getByKeys(java.lang.Class, java.lang.Iterable)
	 */
	@Override
	public <T> SienaFuture<List<T>> getByKeys(Class<T> pClazz, Iterable<?> pKeys)
	{
		return new SienaFutureMock<List<T>>(mSyncPM.getByKeys(pClazz, pKeys));
	}

	/**
	 * @see siena.core.async.PersistenceManagerAsync#update(java.lang.Object[])
	 */
	@Override
	public SienaFuture<Integer> update(Object... pObjects)
	{
		return new SienaFutureMock<Integer>(mSyncPM.update(pObjects));
	}

	/**
	 * @see siena.core.async.PersistenceManagerAsync#update(java.lang.Iterable)
	 */
	@Override
	public <T> SienaFuture<Integer> update(Iterable<T> pObjects)
	{
		return new SienaFutureMock<Integer>(mSyncPM.update(pObjects));
	}

	/**
	 * @see siena.core.async.PersistenceManagerAsync#beginTransaction(int)
	 */
	@Override
	public SienaFuture<Void> beginTransaction(int pIsolationLevel)
	{
		mSyncPM.beginTransaction(pIsolationLevel);
		return ConstantSienaFuture.sVOID;
	}

	/**
	 * @see siena.core.async.PersistenceManagerAsync#beginTransaction()
	 */
	@Override
	public SienaFuture<Void> beginTransaction()
	{
		mSyncPM.beginTransaction();
		return ConstantSienaFuture.sVOID;
	}

	/**
	 * @see siena.core.async.PersistenceManagerAsync#commitTransaction()
	 */
	@Override
	public SienaFuture<Void> commitTransaction()
	{
		mSyncPM.commitTransaction();
		return ConstantSienaFuture.sVOID;
	}

	/**
	 * @see siena.core.async.PersistenceManagerAsync#rollbackTransaction()
	 */
	@Override
	public SienaFuture<Void> rollbackTransaction()
	{
		mSyncPM.rollbackTransaction();
		return ConstantSienaFuture.sVOID;
	}

	/**
	 * @see siena.core.async.PersistenceManagerAsync#closeConnection()
	 */
	@Override
	public SienaFuture<Void> closeConnection()
	{
		mSyncPM.closeConnection();
		return ConstantSienaFuture.sVOID;
	}

	/**
	 * @see siena.core.async.PersistenceManagerAsync#fetch(siena.core.async.QueryAsync)
	 */
	@Override
	public <T> SienaFuture<List<T>> fetch(QueryAsync<T> pQuery)
	{
		@SuppressWarnings("unchecked")
		BaseQueryData<T> queryData = (BaseQueryData<T>) pQuery;
		return new SienaFutureMock<List<T>>(mSyncPM.fetch(queryData, -1, null));
	}

	/**
	 * @see siena.core.async.PersistenceManagerAsync#fetch(siena.core.async.QueryAsync, int)
	 */
	@Override
	public <T> SienaFuture<List<T>> fetch(QueryAsync<T> pQuery, int pLimit)
	{
		@SuppressWarnings("unchecked")
		BaseQueryData<T> queryData = (BaseQueryData<T>) pQuery;
		return new SienaFutureMock<List<T>>(mSyncPM.fetch(queryData, pLimit, null));
	}

	/**
	 * @see siena.core.async.PersistenceManagerAsync#fetch(siena.core.async.QueryAsync, int, java.lang.Object)
	 */
	@Override
	public <T> SienaFuture<List<T>> fetch(QueryAsync<T> pQuery, int pLimit, Object pOffset)
	{
		@SuppressWarnings("unchecked")
		BaseQueryData<T> queryData = (BaseQueryData<T>) pQuery;
		return new SienaFutureMock<List<T>>(mSyncPM.fetch(queryData, pLimit, pOffset));
	}

	/**
	 * @see siena.core.async.PersistenceManagerAsync#count(siena.core.async.QueryAsync)
	 */
	@Override
	public <T> SienaFuture<Integer> count(QueryAsync<T> pQuery)
	{
		@SuppressWarnings("unchecked")
		BaseQueryData<T> queryData = (BaseQueryData<T>) pQuery;
		return new SienaFutureMock<Integer>(mSyncPM.count(queryData));
	}

	/**
	 * @see siena.core.async.PersistenceManagerAsync#delete(siena.core.async.QueryAsync)
	 */
	@Override
	public <T> SienaFuture<Integer> delete(QueryAsync<T> pQuery)
	{
		@SuppressWarnings("unchecked")
		BaseQueryData<T> queryData = (BaseQueryData<T>) pQuery;
		return new SienaFutureMock<Integer>(mSyncPM.delete(queryData));
	}

	/**
	 * @see siena.core.async.PersistenceManagerAsync#fetchKeys(siena.core.async.QueryAsync)
	 */
	@Override
	public <T> SienaFuture<List<T>> fetchKeys(QueryAsync<T> pQuery)
	{
		@SuppressWarnings("unchecked")
		BaseQueryData<T> queryData = (BaseQueryData<T>) pQuery;
		return new SienaFutureMock<List<T>>(mSyncPM.fetchKeys(queryData, -1, null));
	}

	/**
	 * @see siena.core.async.PersistenceManagerAsync#fetchKeys(siena.core.async.QueryAsync, int)
	 */
	@Override
	public <T> SienaFuture<List<T>> fetchKeys(QueryAsync<T> pQuery, int pLimit)
	{
		@SuppressWarnings("unchecked")
		BaseQueryData<T> queryData = (BaseQueryData<T>) pQuery;
		return new SienaFutureMock<List<T>>(mSyncPM.fetchKeys(queryData, pLimit, null));
	}

	/**
	 * @see siena.core.async.PersistenceManagerAsync#fetchKeys(siena.core.async.QueryAsync, int, java.lang.Object)
	 */
	@Override
	public <T> SienaFuture<List<T>> fetchKeys(QueryAsync<T> pQuery, int pLimit, Object pOffset)
	{
		@SuppressWarnings("unchecked")
		BaseQueryData<T> queryData = (BaseQueryData<T>) pQuery;
		return new SienaFutureMock<List<T>>(mSyncPM.fetchKeys(queryData, pLimit, pOffset));
	}

	/**
	 * @see siena.core.async.PersistenceManagerAsync#iter(siena.core.async.QueryAsync)
	 */
	@Override
	public <T> SienaFuture<Iterable<T>> iter(QueryAsync<T> pQuery)
	{
		@SuppressWarnings("unchecked")
		BaseQueryData<T> queryData = (BaseQueryData<T>) pQuery;
		return new SienaFutureMock<Iterable<T>>(mSyncPM.iter(queryData, -1, null));
	}

	/**
	 * @see siena.core.async.PersistenceManagerAsync#iter(siena.core.async.QueryAsync, int)
	 */
	@Override
	public <T> SienaFuture<Iterable<T>> iter(QueryAsync<T> pQuery, int pLimit)
	{
		@SuppressWarnings("unchecked")
		BaseQueryData<T> queryData = (BaseQueryData<T>) pQuery;
		return new SienaFutureMock<Iterable<T>>(mSyncPM.iter(queryData, pLimit, null));
	}

	/**
	 * @see siena.core.async.PersistenceManagerAsync#iter(siena.core.async.QueryAsync, int, java.lang.Object)
	 */
	@Override
	public <T> SienaFuture<Iterable<T>> iter(QueryAsync<T> pQuery, int pLimit, Object pOffset)
	{
		@SuppressWarnings("unchecked")
		BaseQueryData<T> queryData = (BaseQueryData<T>) pQuery;
		return new SienaFutureMock<Iterable<T>>(mSyncPM.iter(queryData, pLimit, pOffset));
	}

	/**
	 * @see siena.core.async.PersistenceManagerAsync#iterPerPage(siena.core.async.QueryAsync, int)
	 */
	@Override
	public <T> SienaFuture<Iterable<T>> iterPerPage(QueryAsync<T> pQuery, int pPageSize)
	{
		@SuppressWarnings("unchecked")
		BaseQueryData<T> queryData = (BaseQueryData<T>) pQuery;
		return new SienaFutureMock<Iterable<T>>(mSyncPM.iterPerPage(queryData, pPageSize));
	}

	/**
	 * @see siena.core.async.PersistenceManagerAsync#paginate(siena.core.async.QueryAsync)
	 */
	@Override
	public <T> void paginate(QueryAsync<T> pQuery)
	{
		@SuppressWarnings("unchecked")
		BaseQueryData<T> queryData = (BaseQueryData<T>) pQuery;
		mSyncPM.paginate(queryData);
	}

	/**
	 * @see siena.core.async.PersistenceManagerAsync#nextPage(siena.core.async.QueryAsync)
	 */
	@Override
	public <T> void nextPage(QueryAsync<T> pQuery)
	{
		@SuppressWarnings("unchecked")
		BaseQueryData<T> queryData = (BaseQueryData<T>) pQuery;
		mSyncPM.nextPage(queryData);
	}

	/**
	 * @see siena.core.async.PersistenceManagerAsync#previousPage(siena.core.async.QueryAsync)
	 */
	@Override
	public <T> void previousPage(QueryAsync<T> pQuery)
	{
		@SuppressWarnings("unchecked")
		BaseQueryData<T> queryData = (BaseQueryData<T>) pQuery;
		mSyncPM.previousPage(queryData);
	}

	/**
	 * @see siena.core.async.PersistenceManagerAsync#update(siena.core.async.QueryAsync, java.util.Map)
	 */
	@Override
	public <T> SienaFuture<Integer> update(QueryAsync<T> pQuery, Map<String, ?> pFieldValues)
	{
		@SuppressWarnings("unchecked")
		BaseQueryData<T> queryData = (BaseQueryData<T>) pQuery;
		return new SienaFutureMock<Integer>(mSyncPM.update(queryData, pFieldValues));
	}

	/**
	 * @see siena.core.async.PersistenceManagerAsync#supportedOperators()
	 */
	@Override
	public String[] supportedOperators()
	{
		return mSyncPM.supportedOperators();
	}
}