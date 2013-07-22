package siena.redis;

import siena.BaseQueryData;
import siena.core.base.BaseQueryInfo;

public class RedisQueryInfo<T> extends BaseQueryInfo<T, RedisClassInfo>
{

	public RedisQueryInfo(RedisClassInfo pCLInfo, BaseQueryData<T> pQuery, boolean pKeysOnly)
	{
		super(pCLInfo, pQuery, pKeysOnly);
	}

}
