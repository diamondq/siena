package siena.core.base;

import java.util.Map;

import com.google.common.base.Function;

public class MapToObjectTransformer<EXTCLASSINFO extends ExtClassInfo, EXTQUERYINFO extends BaseQueryInfo<EXTCLASSINFO>>
		implements Function<Map<String, Object>, Object> {

	private EXTCLASSINFO mCLInfo;
	private boolean mKeysOnly;
	private BasePersistenceManager<EXTCLASSINFO, EXTQUERYINFO, Map<String, Object>> mPM;

	public MapToObjectTransformer(
			boolean pKeysOnly,
			BasePersistenceManager<EXTCLASSINFO, EXTQUERYINFO, Map<String, Object>> pPM,
			EXTCLASSINFO pCLInfo) {
		mCLInfo = pCLInfo;
		mPM = pPM;
		mKeysOnly = pKeysOnly;
	}

	@Override
	public Object apply(Map<String, Object> pInput) {
		if (mKeysOnly == true) {
			return mPM.convertFromMap(mCLInfo, pInput, true);
		} else
			return mPM.convertFromMap(mCLInfo, pInput, false);
	}

}
