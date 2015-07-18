package siena.mapdb.test;

import java.io.File;
import java.io.FileFilter;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;

import org.mapdb.DBMaker;

import siena.PersistenceManager;
import siena.base.test.BaseTest;
import siena.mapdb.MapDBPersistenceManager;
import siena.mapdb.MapDBPersistenceManager.TransactionType;

public class MapDBTest extends BaseTest {

	protected File mTempFile;

	@Override
	public PersistenceManager createPersistenceManager(List<Class<?>> classes)
			throws Exception {

		/* Define the basic environment */

		mTempFile = File.createTempFile("Siena", ".db");
		DBMaker dbMaker = DBMaker.newFileDB(mTempFile);

		MapDBPersistenceManager newPM = new MapDBPersistenceManager(dbMaker,
				TransactionType.AUTO);
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
		File[] delFiles = mTempFile.getParentFile().listFiles(new FileFilter() {

			@Override
			public boolean accept(File pArg) {
				if (pArg.getName().startsWith(mTempFile.getName()))
					return true;
				return false;
			}
		});
		for (File f : delFiles)
			if (f.delete() == false)
				f.deleteOnExit();
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
