package siena.redis;

import java.util.Map;

import siena.core.base.BasePersistenceManager;
import siena.core.base.BasePersistenceManagerAsync;
import siena.redis.RedisPersistenceManager.TransScope;

public class RedisPersistenceManagerAsync extends
	BasePersistenceManagerAsync<RedisClassInfo, RedisQueryInfo<?>, TransScope, Map<String, String>>
{

	public RedisPersistenceManagerAsync(
		BasePersistenceManager<RedisClassInfo, RedisQueryInfo<?>, TransScope, Map<String, String>> pSyncPM)
	{
		super(pSyncPM);
	}

	public void exit()
	{
		((RedisPersistenceManager) mSyncPM).exit();
	}

}
