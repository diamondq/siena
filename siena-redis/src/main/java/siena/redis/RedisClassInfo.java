package siena.redis;

import java.lang.reflect.Field;

import siena.Generator;
import siena.Id;
import siena.core.base.ExtClassInfo;

public class RedisClassInfo extends ExtClassInfo
{

	public Generator	mIDGenerator;

	public RedisClassInfo(Class<?> pClass, RedisPersistenceManager pPM)
	{
		super(pClass);

		/* Verify some of the properties */

		/* Check the type of ID field */

		if (mSienaInfo.keys.isEmpty() == false)
		{
			Field idField = mSienaInfo.keys.get(0);
			Id id = idField.getAnnotation(Id.class);
			mIDGenerator = id.value();
			if (mIDGenerator == Generator.SEQUENCE)
				throw new RuntimeException("Only NONE, UUID and AUTO_GENERATE are valid ID generators for MapDB"); //$NON-NLS-1$
		}

	}
}
