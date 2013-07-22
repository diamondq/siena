package siena.core.base;

import java.util.List;

import siena.QueryJoin;
import siena.Util;

import com.google.common.base.Function;

public class LoadJoinsFunction<EXTCLASSINFO extends ExtClassInfo, EXTQUERYINFO extends BaseQueryInfo<?, EXTCLASSINFO>, EXTTRANSSCOPE extends ExtTransScope, EXTOBJECT, O>
	implements Function<O, O>
{

	private List<QueryJoin>																	mJoins;
	private BasePersistenceManager<EXTCLASSINFO, EXTQUERYINFO, EXTTRANSSCOPE, EXTOBJECT>	mPM;

	public LoadJoinsFunction(BasePersistenceManager<EXTCLASSINFO, EXTQUERYINFO, EXTTRANSSCOPE, EXTOBJECT> pPM,
		List<QueryJoin> pJoins)
	{
		mPM = pPM;
		mJoins = pJoins;
	}

	@Override
	public O apply(O from)
	{
		for (QueryJoin j : mJoins)
		{
			Object newObj = Util.readField(from, j.field);
			mPM.get(newObj);
		}
		return from;
	}

}
