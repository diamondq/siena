package siena.core.base;

import java.lang.reflect.Field;
import java.util.Map;

import com.google.common.base.Function;

public class MapToKeyTransformer<EXTCLASSINFO extends ExtClassInfo, EXTQUERYINFO extends BaseQueryInfo<EXTCLASSINFO>>
		implements Function<Map<String, Object>, Object> {

	private EXTCLASSINFO mCLInfo;

	public MapToKeyTransformer(
			BasePersistenceManager<EXTCLASSINFO, EXTQUERYINFO, Map<String, Object>> pPM,
			EXTCLASSINFO pCLInfo) {
		mCLInfo = pCLInfo;
	}

	@Override
	public Object apply(Map<String, Object> pInput) {
		int keyCount = mCLInfo.mSienaInfo.keys.size();
		if (keyCount > 1) {
			Object[] keys = new Object[keyCount];
			for (int i = 0; i < keyCount; i++) {
				Field keyField = mCLInfo.mSienaInfo.keys.get(i);
				keys[i] = pInput.get(mCLInfo.mFieldToColumnNameMap
						.get(keyField));
			}
			return keys;
		} else {
			Field keyField = mCLInfo.mSienaInfo.keys.get(0);
			return pInput.get(mCLInfo.mFieldToColumnNameMap.get(keyField));
		}
	}

}
