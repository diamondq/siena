package siena.core.base;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import siena.AbstractPersistenceManager;
import siena.ClassInfo;
import siena.Generator;
import siena.Id;
import siena.Query;
import siena.QueryFilter;
import siena.QueryFilterSimple;
import siena.QueryOrder;
import siena.core.async.PersistenceManagerAsync;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;

public abstract class BasePersistenceManager<EXTCLASSINFO extends ExtClassInfo, EXTQUERYINFO extends BaseQueryInfo<EXTCLASSINFO>, EXTOBJECT>
		extends AbstractPersistenceManager {

	public static class MapData {
		public String name;
		public String type;
		public Object key;
		public Map<String, Object> data;
	}

	public static class Pair<A, B> {
		public Pair(A pA, B pB) {
			a = pA;
			b = pB;
		}

		public A a;
		public B b;
	}

	protected Function<EXTOBJECT, EXTOBJECT> sNOPTransformers = Functions
			.identity();

	protected abstract EXTCLASSINFO getClassInfo(Class<?> pClass,
			boolean pCreateIfMissing);

	protected abstract EXTQUERYINFO constructQueryInfo();

	protected <T> EXTQUERYINFO expandQuery(EXTCLASSINFO pCCInfo,
			Query<T> pQuery, boolean pKeysOnly) {
		final EXTQUERYINFO result = constructQueryInfo();
		result.mQuery = pQuery;
		result.mCCInfo = pCCInfo;
		result.mKeysOnly = pKeysOnly;
		return result;
	}

	@SuppressWarnings("unchecked")
	protected String collapseKeyToString(Object key) {
		if (key instanceof Map) {
			StringBuilder sb = new StringBuilder();
			for (Map.Entry<String, Object> pair : ((Map<String, Object>) key)
					.entrySet()) {
				sb.append(pair.getKey());
				sb.append('=');
				sb.append(pair.getValue());
				sb.append('&');
			}
			return sb.toString();
		} else if (key instanceof String)
			return (String) key;
		else
			return key.toString();
	}

	@SuppressWarnings("unchecked")
	public <T> T convertFromMap(EXTCLASSINFO clInfo,
			Map<String, Object> mapData, boolean pOnlyKeys) {
		try {
			Object newObj = clInfo.mSienaInfo.clazz.newInstance();
			List<Field> fields;
			if (pOnlyKeys == true)
				fields = clInfo.mSienaInfo.keys;
			else
				fields = clInfo.mSienaInfo.allFields;
			for (Field f : fields) {
				Object value = mapData.get(clInfo.mFieldToColumnNameMap.get(f));
				f.set(newObj, value);
			}

			return (T) newObj;
		} catch (InstantiationException ex) {
			throw new RuntimeException(ex);
		} catch (IllegalAccessException ex) {
			throw new RuntimeException(ex);
		}
	}

	protected abstract <T> T generateUUID(Class<T> pUUIDClass);

	@SuppressWarnings("unchecked")
	public List<MapData> convertToMap(EXTCLASSINFO clInfo, Object obj) {
		List<MapData> results = new ArrayList<MapData>();
		MapData primary = new MapData();
		results.add(primary);
		primary.name = clInfo.mSienaInfo.tableName;
		primary.type = "table"; //$NON-NLS-1$
		primary.data = new HashMap<String, Object>();
		Field prevIdField = null;

		try {
			for (Field f : clInfo.mSienaInfo.allFields) {
				Object value = f.get(obj);

				/* Handle ID fields */

				if (ClassInfo.isId(f) == true) {
					Generator generator = f.getAnnotation(Id.class).value();
					if (generator == Generator.UUID) {
						/* If the UUID is not set, then create one now */

						if (value == null) {
							value = generateUUID(f.getType());

							/* Update the original object with the new UUID */

							f.set(obj, value);
						}
					} else if ((generator == Generator.AUTO_INCREMENT)
							|| (generator == Generator.SEQUENCE)) {
						boolean inc = false;
						if (value == null)
							inc = true;
						else if (value instanceof Short)
							inc = ((Short) value).shortValue() == 0 ? true
									: false;
						else if (value instanceof Integer)
							inc = ((Integer) value).intValue() == 0 ? true
									: false;
						else if (value instanceof Long)
							inc = ((Long) value).longValue() == 0 ? true
									: false;
						if (inc == true) {
							long idOffset = generateNextSequence(clInfo,
									clInfo.mSienaInfo.tableName,
									clInfo.mFieldToColumnNameMap.get(f));

							/* Update the original object with the new UUID */

							f.set(obj, idOffset);
							value = idOffset;
						}
					}
					if (primary.key == null) {
						primary.key = value;
						prevIdField = f;
					} else if (primary.key instanceof Map)
						((Map<String, Object>) primary.key).put(
								clInfo.mFieldToColumnNameMap.get(f), value);
					else {
						Map<String, Object> m = new HashMap<String, Object>();
						m.put(clInfo.mFieldToColumnNameMap.get(prevIdField),
								primary.key);
						m.put(clInfo.mFieldToColumnNameMap.get(f), value);
						primary.key = m;
					}
				}
				primary.data.put(clInfo.mFieldToColumnNameMap.get(f), value);
			}
		} catch (IllegalArgumentException ex) {
			throw new RuntimeException(ex);
		} catch (IllegalAccessException ex) {
			throw new RuntimeException(ex);
		}

		return results;
	}

	protected abstract long generateNextSequence(EXTCLASSINFO pClassInfo,
			String pTableName, String pColumnName);

	/**
	 * @see siena.PersistenceManager#insert(java.lang.Object[])
	 */
	@Override
	public int insert(Object... objects) {
		if (objects != null) {
			for (Object obj : objects)
				insert(obj);
			return objects.length;
		}
		return 0;
	}

	/**
	 * @see siena.PersistenceManager#insert(java.lang.Iterable)
	 */
	@Override
	public int insert(Iterable<?> objects) {
		if (objects != null) {
			int count = 0;
			for (Object obj : objects) {
				insert(obj);
				count++;
			}
			return count;
		}
		return 0;
	}

	@Override
	public int save(Object... objects) {
		if (objects != null) {
			for (Object obj : objects)
				save(obj);
			return objects.length;
		}
		return 0;
	}

	@Override
	public int save(Iterable<?> objects) {
		if (objects != null) {
			int count = 0;
			for (Object obj : objects) {
				save(obj);
				count++;
			}
			return count;
		}
		return 0;
	}

	@Override
	public void save(Object obj) {
		insert(obj);
	}

	protected abstract <T, O> Iterator<O> retrieveData(EXTCLASSINFO pCLInfo,
			Query<T> pQuery, EXTQUERYINFO pQueryInfo, int pLimit,
			Object pOffset, Function<EXTOBJECT, O> pTransformers);

	/**
	 * @see siena.PersistenceManager#delete(siena.Query)
	 */
	@Override
	public <T> int delete(Query<T> pQuery) {
		EXTCLASSINFO clInfo = getClassInfo(pQuery.getQueriedClass(), true);
		EXTQUERYINFO queryInfo = expandQuery(clInfo, pQuery, true);
		Iterator<Object> iterator = retrieveData(clInfo, pQuery, queryInfo, -1,
				null, getTransformerToKeys(clInfo));
		return deleteByKeys(pQuery.getQueriedClass(), iterator);
	}

	/**
	 * @see siena.PersistenceManager#count(siena.Query)
	 */
	@Override
	public <T> int count(Query<T> pQuery) {
		EXTCLASSINFO clInfo = getClassInfo(pQuery.getQueriedClass(), true);
		EXTQUERYINFO queryInfo = expandQuery(clInfo, pQuery, true);
		Iterator<EXTOBJECT> iterator = retrieveData(clInfo, pQuery, queryInfo,
				-1, null, sNOPTransformers);
		int count = 0;
		for (; iterator.hasNext();) {
			iterator.next();
			count++;
		}
		return count;
	}

	/**
	 * @see siena.core.base.BasePersistenceManager#fetch(siena.Query, int,
	 *      java.lang.Object)
	 */
	@Override
	public <T> List<T> fetch(Query<T> pQuery, int pLimit, Object pOffset) {
		EXTCLASSINFO clInfo = getClassInfo(pQuery.getQueriedClass(), true);
		EXTQUERYINFO queryInfo = expandQuery(clInfo, pQuery, false);
		Function<EXTOBJECT, T> transformerToObject = getTransformerToObject(clInfo);
		Iterator<T> iterator = retrieveData(clInfo, pQuery, queryInfo, pLimit,
				pOffset, transformerToObject);
		List<T> results = Lists.newArrayList(iterator);
		return results;
	}

	/**
	 * @see siena.core.base.BasePersistenceManager#iter(siena.Query, int,
	 *      java.lang.Object)
	 */
	@Override
	public <T> Iterable<T> iter(Query<T> pQuery, int pLimit, Object pOffset) {
		EXTCLASSINFO clInfo = getClassInfo(pQuery.getQueriedClass(), true);
		EXTQUERYINFO queryInfo = expandQuery(clInfo, pQuery, false);
		Function<EXTOBJECT, T> transformerToObject = getTransformerToObject(clInfo);
		Iterator<T> iterator = retrieveData(clInfo, pQuery, queryInfo, pLimit,
				pOffset, transformerToObject);
		@SuppressWarnings("unchecked")
		Iterable<T> r = (Iterable<T>) iterator;
		return r;
	}

	/**
	 * @see siena.core.base.BasePersistenceManager#fetchKeys(siena.Query, int,
	 *      java.lang.Object)
	 */
	@Override
	public <T> List<T> fetchKeys(Query<T> pQuery, int pLimit, Object pOffset) {
		EXTCLASSINFO clInfo = getClassInfo(pQuery.getQueriedClass(), true);
		EXTQUERYINFO queryInfo = expandQuery(clInfo, pQuery, true);
		Function<EXTOBJECT, T> transformerToObjectKeys = getTransformerToObjectKeys(clInfo);
		Iterator<T> iterator = retrieveData(clInfo, pQuery, queryInfo, pLimit,
				pOffset, transformerToObjectKeys);
		List<T> results = Lists.newArrayList(iterator);
		return results;
	}

	protected abstract <T> Function<EXTOBJECT, T> getTransformerToObject(
			EXTCLASSINFO pCLInfo);

	protected abstract <T> Function<EXTOBJECT, T> getTransformerToObjectKeys(
			EXTCLASSINFO pCLInfo);

	protected abstract Function<EXTOBJECT, Object> getTransformerToKeys(
			EXTCLASSINFO pCLInfo);

	@Override
	public int delete(Object... objects) {
		if (objects != null) {
			for (Object obj : objects)
				delete(obj);
			return objects.length;
		}
		return 0;
	}

	@Override
	public int delete(Iterable<?> objects) {
		if (objects != null) {
			int count = 0;
			for (Object obj : objects) {
				delete(obj);
				count++;
			}
			return count;
		}
		return 0;
	}

	@Override
	public <T> int deleteByKeys(Class<T> clazz, Iterable<?> keys) {
		if (keys != null) {
			int count = 0;
			for (Object key : keys) {
				deleteByKey(clazz, key);
				count++;
			}
			return count;
		}

		return 0;
	}

	@Override
	public <T> int deleteByKeys(Class<T> clazz, Object... keys) {
		if (keys != null) {
			for (Object key : keys)
				deleteByKey(clazz, key);
			return keys.length;
		}
		return 0;
	}

	protected abstract <T> void deleteByKey(Class<T> clazz, Object key);

	@Override
	public int get(Object... models) {
		if (models != null) {
			for (Object obj : models)
				get(obj);
			return models.length;
		}
		return 0;
	}

	@Override
	public <T> int get(Iterable<T> models) {
		if (models != null) {
			int count = 0;
			for (Object obj : models) {
				get(obj);
				count++;
			}
			return count;
		}
		return 0;
	}

	@Override
	public <T> List<T> getByKeys(Class<T> clazz, Object... keys) {
		List<T> results = new ArrayList<T>();
		if (keys != null) {
			for (Object key : keys)
				results.add(getByKey(clazz, key));
		}
		return results;
	}

	@Override
	public <T> List<T> getByKeys(Class<T> clazz, Iterable<?> keys) {
		List<T> results = new ArrayList<T>();
		if (keys != null) {
			for (Object key : keys)
				results.add(getByKey(clazz, key));
		}
		return results;
	}

	@Override
	public <T> int update(Object... models) {
		if (models != null) {
			for (Object obj : models)
				update(obj);
			return models.length;
		}
		return 0;
	}

	@Override
	public <T> int update(Iterable<T> models) {
		if (models != null) {
			int count = 0;
			for (Object obj : models) {
				update(obj);
				count++;
			}
			return count;
		}
		return 0;
	}

	@Override
	public String[] supportedOperators() {
		return new String[] { "<", "<=", ">", ">=", "!=", "=", "IN" }; //$NON-NLS-1$//$NON-NLS-2$
	}

	@Override
	public void get(Object obj) {
		throw new IllegalArgumentException("Not yet implemented"); //$NON-NLS-1$
	}

	@Override
	public void delete(Object obj) {
		throw new IllegalArgumentException("Not yet implemented"); //$NON-NLS-1$
	}

	@Override
	public void update(Object obj) {
		throw new IllegalArgumentException("Not yet implemented"); //$NON-NLS-1$
	}

	@Override
	public <T> T getByKey(Class<T> clazz, Object key) {
		throw new IllegalArgumentException("Not yet implemented"); //$NON-NLS-1$
	}

	@Override
	public void beginTransaction(int isolationLevel) {
		throw new IllegalArgumentException("Not yet implemented"); //$NON-NLS-1$
	}

	@Override
	public void beginTransaction() {
		throw new IllegalArgumentException("Not yet implemented"); //$NON-NLS-1$
	}

	@Override
	public void commitTransaction() {
		throw new IllegalArgumentException("Not yet implemented"); //$NON-NLS-1$
	}

	@Override
	public void rollbackTransaction() {
		throw new IllegalArgumentException("Not yet implemented"); //$NON-NLS-1$
	}

	@Override
	public void closeConnection() {
		throw new IllegalArgumentException("Not yet implemented"); //$NON-NLS-1$
	}

	@Override
	public <T> int update(Query<T> query, Map<String, ?> fieldValues) {
		throw new IllegalArgumentException("Not yet implemented"); //$NON-NLS-1$
	}

	@Override
	public <T> List<T> fetch(Query<T> query) {
		return fetch(query, -1, null);
	}

	@Override
	public <T> List<T> fetch(Query<T> query, int limit) {
		return fetch(query, limit, null);
	}

	@Override
	public <T> List<T> fetchKeys(Query<T> query) {
		return fetchKeys(query, -1, null);
	}

	@Override
	public <T> List<T> fetchKeys(Query<T> query, int limit) {
		return fetchKeys(query, limit, null);
	}

	@Override
	public <T> Iterable<T> iter(Query<T> query) {
		return iter(query, -1, null);
	}

	@Override
	public <T> Iterable<T> iter(Query<T> query, int limit) {
		return iter(query, limit, null);
	}

	@Override
	public <T> void paginate(Query<T> query) {
		throw new IllegalArgumentException("Not yet implemented"); //$NON-NLS-1$
	}

	@Override
	public <T> void nextPage(Query<T> query) {
		throw new IllegalArgumentException("Not yet implemented"); //$NON-NLS-1$
	}

	@Override
	public <T> void previousPage(Query<T> query) {
		throw new IllegalArgumentException("Not yet implemented"); //$NON-NLS-1$
	}

	@Override
	public <T> PersistenceManagerAsync async() {
		throw new IllegalArgumentException("Not yet implemented"); //$NON-NLS-1$
	}

	/**
	 * This method returns an iterator for the given source of data where the
	 * data has been transformed and then sorted
	 * 
	 * @param pSource
	 *            the original source of data
	 * @param pQuery
	 *            the query
	 * @param pQueryInfo
	 *            the query info
	 * @param pTransformer
	 *            the transformer
	 * @return the iterator of sorted, transformed data
	 */
	@SuppressWarnings("unchecked")
	protected <O, T> Iterator<O> buildSortedIterator(
			Iterable<EXTOBJECT> pSource, Query<T> pQuery,
			final EXTCLASSINFO pCLInfo, EXTQUERYINFO pQueryInfo,
			Function<EXTOBJECT, O> pTransformer) {
		final List<QueryOrder> orderBys = pQuery.getOrders();
		if ((orderBys == null) || (orderBys.size() == 0))
			return Iterators.transform(pSource.iterator(), pTransformer);
		else {
			Ordering<EXTOBJECT> o = new Ordering<EXTOBJECT>() {

				@Override
				public int compare(EXTOBJECT left, EXTOBJECT right) {
					for (QueryOrder qo : orderBys) {
						Object obj0;
						Object obj1;
						obj0 = BasePersistenceManager.this.getFromRaw(left,
								pCLInfo, qo.field);
						obj1 = BasePersistenceManager.this.getFromRaw(right,
								pCLInfo, qo.field);
						if (obj0 == null) {
							if (obj1 != null)
								return qo.ascending == true ? -1 : 1;
						} else {
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
			List<EXTOBJECT> sortedCopy = o.sortedCopy(pSource);
			Iterable<O> transform = Iterables.transform(sortedCopy,
					pTransformer);
			return transform.iterator();
		}
	}

	/**
	 * Checks to see if a given object in raw format matches the values in the
	 * query
	 * 
	 * @param pObj
	 *            the object in raw format
	 * @param pCLInfo
	 *            the class
	 * @param pQuery
	 *            the query
	 * @param pQueryInfo
	 *            the query info
	 * @return true or false
	 */
	protected boolean checkAgainstQuery(EXTOBJECT pObj, EXTCLASSINFO pCLInfo,
			EXTQUERYINFO pQueryInfo) {
		List<QueryFilter> filters = pQueryInfo.mQuery.getFilters();
		for (QueryFilter f : filters) {
			if (f instanceof QueryFilterSimple) {
				QueryFilterSimple qfs = (QueryFilterSimple) f;
				Object rawValue = getFromRaw(pObj, pCLInfo, qfs.field);
				if (qfs.operator.equals("=")) {
					if (!(((rawValue == null) && (qfs.value == null)) || (rawValue
							.equals(qfs.value))))
						return false;
				} else if (qfs.operator.equals("!=")) {
					if (((rawValue == null) && (qfs.value == null))
							|| (rawValue.equals(qfs.value)))
						return false;
				} else if (qfs.operator.equals("IN")) {
					if (qfs.value instanceof Iterable) {
						boolean match = false;
						@SuppressWarnings("unchecked")
						Iterable<Object> io = (Iterable<Object>) qfs.value;
						for (Object o : io) {
							if (((rawValue == null) && (o == null))
									|| (rawValue.equals(o))) {
								match = true;
								break;
							}
						}
						if (match == false)
							return false;
					}
				} else if (qfs.operator.equals("<")
						|| qfs.operator.equals("<=")
						|| qfs.operator.equals(">")
						|| qfs.operator.equals(">=")) {
					int r;
					if ((rawValue instanceof Number)
							|| (qfs.value instanceof Number)) {
						Number num1;
						Number num2;
						if (rawValue instanceof Number)
							num1 = (Number) rawValue;
						else if (rawValue instanceof String) {
							num1 = Double.parseDouble((String) rawValue);
						} else
							return false;
						if (qfs.value instanceof Number)
							num2 = (Number) qfs.value;
						else if (qfs.value instanceof String) {
							num2 = Double.parseDouble((String) qfs.value);
						} else
							return false;

						if (num1.getClass().equals(num2.getClass())) {
							@SuppressWarnings("unchecked")
							Comparable<Number> c1 = (Comparable<Number>) num1;
							r = c1.compareTo(num2);
						} else {
							if ((num1 instanceof BigDecimal)
									|| (num2 instanceof BigDecimal)
									|| (num1 instanceof BigInteger)
									|| (num2 instanceof BigInteger)) {
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
							} else if ((num1 instanceof Float)
									|| (num1 instanceof Double)
									|| (num2 instanceof Float)
									|| (num2 instanceof Double)) {
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
							} else {
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
					} else {
						String s1 = rawValue.toString();
						String s2 = qfs.value.toString();
						r = s1.compareTo(s2);
					}
					if (qfs.operator.equals("<")) {
						if (r >= 0)
							return false;
					} else if (qfs.operator.equals("<=")) {
						if (r > 0)
							return false;
					} else if (qfs.operator.equals(">")) {
						if (r <= 0)
							return false;
					} else if (qfs.operator.equals(">=")) {
						if (r < 0)
							return false;
					}
				}
			} else
				throw new IllegalArgumentException();
		}
		return true;
	}

	protected abstract Object getFromRaw(EXTOBJECT pObj, EXTCLASSINFO pCLInfo,
			Field pField);
}
