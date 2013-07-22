package siena.core.base;

import siena.core.options.QueryOption;

public class QueryOptionCursorDetails extends QueryOption
{

	public static final int	ID				= 0x5001;

	public int				pageOffset		= 0;

	public int				cursorOffset	= 0;
	public int				cursorLimit		= Integer.MAX_VALUE;

	public boolean			isAtBeginning	= false;
	public boolean			isAtEnd			= false;

	public QueryOptionCursorDetails()
	{
		super(ID);
	}

	@Override
	public QueryOption clone()
	{
		QueryOptionCursorDetails r = new QueryOptionCursorDetails();
		r.type = type;
		r.state = state;
		r.pageOffset = pageOffset;
		r.isAtBeginning = isAtBeginning;
		r.isAtEnd = isAtEnd;
		r.cursorOffset = cursorOffset;
		r.cursorLimit = cursorLimit;
		return r;
	}

}
