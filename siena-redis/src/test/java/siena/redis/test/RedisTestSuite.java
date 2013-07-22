package siena.redis.test;

import org.junit.BeforeClass;

import siena.base.test.GenericSuite;
import siena.base.test.GenericTestSetup;

public class RedisTestSuite extends GenericSuite
{
	@BeforeClass
	public static void setupSuite()
	{
		GenericTestSetup.setup = new RedisTestSetup();
	}
}
