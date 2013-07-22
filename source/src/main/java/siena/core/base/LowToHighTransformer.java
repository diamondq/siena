package siena.core.base;

import com.google.common.base.Function;

public class LowToHighTransformer<EXTCLASSINFO extends ExtClassInfo, EXTQUERYINFO extends BaseQueryInfo<?, EXTCLASSINFO>, EXTTRANSSCOPE extends ExtTransScope, EXTOBJECT>
	implements Function<EXTOBJECT, Object>
{

	private EXTCLASSINFO																	mCLInfo;
	private boolean																			mKeysOnly;
	private BasePersistenceManager<EXTCLASSINFO, EXTQUERYINFO, EXTTRANSSCOPE, EXTOBJECT>	mPM;

	public LowToHighTransformer(boolean pKeysOnly,
		BasePersistenceManager<EXTCLASSINFO, EXTQUERYINFO, EXTTRANSSCOPE, EXTOBJECT> pPM, EXTCLASSINFO pCLInfo)
	{
		mCLInfo = pCLInfo;
		mPM = pPM;
		mKeysOnly = pKeysOnly;
	}

	@Override
	public Object apply(EXTOBJECT pInput)
	{
		if (mKeysOnly == true)
		{
			return mPM.convertFromRaw(mCLInfo, pInput, true, false);
		}
		else
			return mPM.convertFromRaw(mCLInfo, pInput, false, false);
	}

}
