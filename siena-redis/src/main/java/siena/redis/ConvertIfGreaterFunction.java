package siena.redis;

import redis.clients.jedis.Response;

import com.google.common.base.Function;

public class ConvertIfGreaterFunction implements Function<Response<Long>, Boolean>
{

	@Override
	public Boolean apply(Response<Long> pInput)
	{
		if (pInput.get() > 0)
			return true;
		else
			return false;
	}

}
