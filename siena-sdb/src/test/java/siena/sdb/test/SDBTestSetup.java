package siena.sdb.test;

import java.util.List;

import junit.framework.TestCase;
import siena.PersistenceManager;
import siena.base.test.GenericTestSetup;
import siena.base.test.SimpleDBConfig;
import siena.core.async.PersistenceManagerAsync;
import siena.sdb.SdbPersistenceManager;

public class SDBTestSetup extends GenericTestSetup {

	/**
	 * @see siena.base.test.GenericTestSetup#createPersistenceManager(junit.framework.TestCase,
	 *      java.util.List)
	 */
	@Override
	public PersistenceManager createPersistenceManager(TestCase pTest,
			List<Class<?>> classes) throws Exception {
		SdbPersistenceManager sdb = new SdbPersistenceManager();
		sdb.init(SimpleDBConfig.getSienaAWSProperties());

		sdb.option(SdbPersistenceManager.CONSISTENT_READ);

		return sdb;
	}

	/**
	 * @see siena.base.test.GenericTestSetup#createPersistenceManagerAsync(junit.framework.TestCase,
	 *      java.util.List)
	 */
	@Override
	public PersistenceManagerAsync createPersistenceManagerAsync(
			TestCase pTest, List<Class<?>> pClasses) throws Exception {
		throw new IllegalArgumentException("Not yet implemented");
	}

	@Override
	public boolean supportsAutoincrement() {
		return false;
	}

	@Override
	public boolean supportsMultipleKeys() {
		return false;
	}

	@Override
	public boolean mustFilterToOrder() {
		return false;
	}

	@Override
	public boolean supportsDeleteException() {
		return false;
	}

	@Override
	public boolean supportsSearchStart() {
		return true;
	}

	@Override
	public boolean supportsSearchEnd() {
		return true;
	}

	@Override
	public boolean supportsTransaction() {
		return false;
	}

	@Override
	public boolean supportsListStore() {
		return false;
	}

	@Override
	public void tearDown(TestCase pTest) throws Exception {

	}

}
