package siena.core.base;

import siena.BaseQuery;
import siena.BaseQueryData;
import siena.PersistenceManager;

public class ExtBaseQuery<T> extends BaseQuery<T>
{

	private static final long	serialVersionUID	= 1462901857083642120L;

	public ExtBaseQuery()
	{
		super();
		addExtraOptions();
	}

	public ExtBaseQuery(BaseQuery<T> pQuery)
	{
		super(pQuery);
		addExtraOptions();
	}

	public ExtBaseQuery(PersistenceManager pPm, BaseQueryData<T> pData)
	{
		super(pPm, pData);
		addExtraOptions();
	}

	public ExtBaseQuery(PersistenceManager pPm, Class<T> pClazz)
	{
		super(pPm, pClazz);
		addExtraOptions();
	}

	@Override
	public void resetOptions()
	{
		super.resetOptions();
		addExtraOptions();
	}

	protected void addExtraOptions()
	{
		options.put(QueryOptionCursorDetails.ID, new QueryOptionCursorDetails());
	}
}
