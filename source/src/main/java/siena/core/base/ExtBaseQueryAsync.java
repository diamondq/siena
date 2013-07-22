package siena.core.base;

import siena.BaseQueryData;
import siena.core.async.BaseQueryAsync;
import siena.core.async.PersistenceManagerAsync;

public class ExtBaseQueryAsync<T> extends BaseQueryAsync<T>
{

	private static final long	serialVersionUID	= -3967148822664251669L;

	public ExtBaseQueryAsync(BaseQueryAsync<T> pQuery)
	{
		super(pQuery);
		addExtraOptions();
	}

	public ExtBaseQueryAsync(PersistenceManagerAsync pPm, BaseQueryData<T> pData)
	{
		super(pPm, pData);
		addExtraOptions();
	}

	public ExtBaseQueryAsync(PersistenceManagerAsync pPm, Class<T> pClazz)
	{
		super(pPm, pClazz);
		addExtraOptions();
	}

	/**
	 * @see siena.BaseQueryData#resetOptions()
	 */
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
