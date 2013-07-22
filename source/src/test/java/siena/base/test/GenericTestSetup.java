package siena.base.test;

import java.util.List;

import junit.framework.TestCase;
import siena.PersistenceManager;
import siena.core.async.PersistenceManagerAsync;

public abstract class GenericTestSetup
{
	public static GenericTestSetup	setup;

	public abstract PersistenceManager createPersistenceManager(TestCase pTest, List<Class<?>> classes)
		throws Exception;

	public abstract PersistenceManagerAsync createPersistenceManagerAsync(TestCase pTest, List<Class<?>> pClasses)
		throws Exception;

	public abstract void tearDown(TestCase pTest) throws Exception;

	public abstract boolean supportsAutoincrement();

	public abstract boolean supportsMultipleKeys();

	public abstract boolean mustFilterToOrder();

	public static void confirmSetup() throws Exception
	{
		if (setup == null)
		{
			String testClass = System.getProperty("GenericTest");
			if (testClass == null)
				throw new IllegalStateException(
					"Unable to launch an individual test because the GenericTest property was not set to the name of the TestSetup class.");
			Class<?> clazz = Class.forName(testClass);
			setup = (GenericTestSetup) clazz.newInstance();
		}
	}

	public abstract boolean supportsDeleteException();

	public abstract boolean supportsSearchStart();

	public abstract boolean supportsSearchEnd();

	public abstract boolean supportsTransaction();

	public abstract boolean supportsListStore();

}
