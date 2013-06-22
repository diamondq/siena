package siena.mapdb.test;

import java.util.HashSet;
import java.util.List;
import java.util.Properties;

import org.mapdb.DB;
import org.mapdb.DBMaker;

import siena.PersistenceManager;
import siena.base.test.BaseTest;
import siena.mapdb.MapDBPersistenceManager;

public class MapDBTest extends BaseTest {

	@SuppressWarnings("unused")
	@Override
	public PersistenceManager createPersistenceManager(List<Class<?>> classes)
			throws Exception {

		/* Define the basic environment */

		DB db = DBMaker.newDirectMemoryDB().make();

		MapDBPersistenceManager newPM = new MapDBPersistenceManager(db);
		Properties properties = new Properties();
		newPM.init(properties);

		/* For each class, create a column family */

		HashSet<String> done = new HashSet<String>();

		return newPM;
	}

	@Override
	protected void tearDown() throws Exception {
		super.tearDown();
		((MapDBPersistenceManager) pm).exit();
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

}
