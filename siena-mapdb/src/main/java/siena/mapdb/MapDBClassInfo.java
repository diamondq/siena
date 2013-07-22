package siena.mapdb;

import java.lang.reflect.Field;

import siena.Generator;
import siena.Id;
import siena.core.base.ExtClassInfo;

public class MapDBClassInfo extends ExtClassInfo {

	public Generator mIDGenerator;

	public MapDBClassInfo(Class<?> pClass, MapDBPersistenceManager pPM) {
		super(pClass);

		/* Verify some of the properties */

		/* Check the type of ID field */

		Field idField = mSienaInfo.getIdField();
		Id id = idField.getAnnotation(Id.class);
		mIDGenerator = id.value();
		if (mIDGenerator == Generator.SEQUENCE)
			throw new RuntimeException(
					"Only NONE, UUID and AUTO_GENERATE are valid ID generators for MapDB"); //$NON-NLS-1$

	}
}
