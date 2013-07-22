package siena.redis.test;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import junit.framework.TestCase;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import siena.PersistenceManager;
import siena.base.test.GenericTestSetup;
import siena.core.async.PersistenceManagerAsync;
import siena.core.base.BasePersistenceManager;
import siena.redis.RedisClassInfo;
import siena.redis.RedisPersistenceManager;
import siena.redis.RedisPersistenceManager.TransScope;
import siena.redis.RedisPersistenceManager.TransactionType;
import siena.redis.RedisPersistenceManagerAsync;
import siena.redis.RedisQueryInfo;

public class RedisTestSetup extends GenericTestSetup
{

	/**
	 * @see siena.base.test.GenericTestSetup#createPersistenceManager(junit.framework.TestCase, java.util.List)
	 */
	@Override
	public PersistenceManager createPersistenceManager(TestCase pTest, List<Class<?>> classes) throws Exception
	{
		JedisPool jedis = new JedisPool(new JedisPoolConfig(), "localhost");

		/* Define the basic environment */

		RedisPersistenceManager newPM = new RedisPersistenceManager(jedis, TransactionType.AUTO);
		Properties properties = new Properties();
		newPM.init(properties);

		return newPM;
	}

	/**
	 * @see siena.base.test.GenericTestSetup#createPersistenceManagerAsync(junit.framework.TestCase, java.util.List)
	 */
	@Override
	public PersistenceManagerAsync createPersistenceManagerAsync(TestCase pTest, List<Class<?>> pClasses)
		throws Exception
	{
		@SuppressWarnings("unchecked")
		RedisPersistenceManagerAsync newPM =
			new RedisPersistenceManagerAsync(
				(BasePersistenceManager<RedisClassInfo, RedisQueryInfo<?>, TransScope, Map<String, String>>) createPersistenceManager(
					pTest, pClasses));
		return newPM;
	}

	@Override
	public void tearDown(TestCase pTest) throws Exception
	{
		Field pmField;
		Class<?> clazz = pTest.getClass();
		do
		{
			try
			{
				pmField = clazz.getDeclaredField("pm");
			}
			catch (NoSuchFieldException ex)
			{
				pmField = null;
			}
			if (pmField == null)
				clazz = clazz.getSuperclass();
		}
		while (pmField == null);

		pmField.setAccessible(true);
		Object pm = pmField.get(pTest);
		if (pm instanceof RedisPersistenceManager)
			((RedisPersistenceManager) pm).exit();
		else if (pm instanceof RedisPersistenceManagerAsync)
			((RedisPersistenceManagerAsync) pm).exit();
	}

	@Override
	public boolean supportsAutoincrement()
	{
		return true;
	}

	@Override
	public boolean supportsMultipleKeys()
	{
		return false;
	}

	@Override
	public boolean mustFilterToOrder()
	{
		return false;
	}

	@Override
	public boolean supportsDeleteException()
	{
		return true;
	}

	@Override
	public boolean supportsSearchStart()
	{
		return true;
	}

	@Override
	public boolean supportsSearchEnd()
	{
		return true;
	}

	@Override
	public boolean supportsTransaction()
	{
		return true;
	}

	@Override
	public boolean supportsListStore()
	{
		return true;
	}

}
