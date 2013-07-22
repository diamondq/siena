package siena.core.base;

import java.lang.reflect.Field;

import com.google.common.base.Function;

public class MapToKeyTransformer<EXTCLASSINFO extends ExtClassInfo, EXTQUERYINFO extends BaseQueryInfo<?, EXTCLASSINFO>, EXTTRANSSCOPE extends ExtTransScope, EXTOBJECT>
	implements Function<EXTOBJECT, EXTOBJECT>
{

	private EXTCLASSINFO																	mCLInfo;
	private BasePersistenceManager<EXTCLASSINFO, EXTQUERYINFO, EXTTRANSSCOPE, EXTOBJECT>	mPM;

	public MapToKeyTransformer(BasePersistenceManager<EXTCLASSINFO, EXTQUERYINFO, EXTTRANSSCOPE, EXTOBJECT> pPM,
		EXTCLASSINFO pCLInfo)
	{
		mPM = pPM;
		mCLInfo = pCLInfo;
	}

	@Override
	public EXTOBJECT apply(EXTOBJECT pInput)
	{
		int keyCount = mCLInfo.mSienaInfo.keys.size();
		EXTOBJECT result = mPM.createLow(mCLInfo);
		for (int i = 0; i < keyCount; i++)
		{
			Field keyField = mCLInfo.mSienaInfo.keys.get(i);
			mPM.setLowColumnValue(result, mCLInfo, keyField,
				mPM.getLowColumnValue(pInput, mCLInfo, keyField.getType(), keyField));
		}
		return result;
	}

}
