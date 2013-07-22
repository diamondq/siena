package siena.core.base;

import siena.core.options.QueryOptionPage;
import siena.core.options.QueryOptionState;

public class BaseOptions
{
	public final QueryOptionPage			pageOpt;
	public final QueryOptionState			stateOpt;
	public final QueryOptionCursorDetails	cursorOpt;

	public BaseOptions(QueryOptionPage pPageOpt, QueryOptionState pStateOpt, QueryOptionCursorDetails pCursorOpt)
	{
		pageOpt = pPageOpt;
		stateOpt = pStateOpt;
		cursorOpt = pCursorOpt;
	}

}
