package siena.mapdb;

import java.lang.reflect.Field;

import org.mapdb.HTreeMap;

import siena.Generator;
import siena.Id;
import siena.core.base.ExtClassInfo;

public class MapDBClassInfo extends ExtClassInfo {

	public Generator mIDGenerator;
	public HTreeMap<Object, Object> mPrimaryMap;
	public HTreeMap<Object, Object> mSecondaryData;
	private MapDBPersistenceManager mPM;

	private HTreeMap<Object, Object> createOrGetMap(String pName) {
		return mPM.mDB.getHashMap(pName);
	}

	public MapDBClassInfo(Class<?> pClass, MapDBPersistenceManager pPM) {
		super(pClass);

		mPM = pPM;
		mPrimaryMap = createOrGetMap(mSienaInfo.tableName);

		/* Verify some of the properties */

		/* Check the type of ID field */

		Field idField = mSienaInfo.getIdField();
		Id id = idField.getAnnotation(Id.class);
		mIDGenerator = id.value();
		if (mIDGenerator == Generator.SEQUENCE)
			throw new RuntimeException(
					"Only NONE, UUID and AUTO_GENERATE are valid ID generators for MapDB"); //$NON-NLS-1$
		else if (mIDGenerator == Generator.AUTO_INCREMENT) {
			mSecondaryData = createOrGetMap(mSienaInfo.tableName + "__SEC"); //$NON-NLS-1$
			if (mSecondaryData.containsKey(mSienaInfo.tableName + "__INC") == false) //$NON-NLS-1$
				mSecondaryData.put(idField.getName() + "__INC", 0L); //$NON-NLS-1$
		}

		/* Query the schema to see what fields have keys */

		//Map<String, Object> componentData = ArezzoSingleton.sINSTANCE.getComponentData("cassandra"); //$NON-NLS-1$
	}
}
