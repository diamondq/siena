package siena.sdb.test;

import org.junit.BeforeClass;

import siena.base.test.GenericSuite;
import siena.base.test.GenericTestSetup;

public class SDBTestSuite extends GenericSuite
{
	@BeforeClass
	public static void setupSuite()
	{
		GenericTestSetup.setup = new SDBTestSetup();
	}
}
