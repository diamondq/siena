package siena.core.base;

import siena.core.async.SienaFuture;

public class ConstantSienaFuture<T> implements SienaFuture<T>
{

	public static final SienaFuture<Boolean>	sBOOLEAN_TRUE	= new ConstantSienaFuture<Boolean>(Boolean.TRUE);
	public static final SienaFuture<Boolean>	sBOOLEAN_FALSE	= new ConstantSienaFuture<Boolean>(Boolean.FALSE);
	public static final SienaFuture<Void>		sVOID			= new ConstantSienaFuture<Void>(null);

	private final T								mVal;

	private ConstantSienaFuture(T pVal)
	{
		mVal = pVal;
	}

	@Override
	public T get()
	{
		return mVal;
	}

}
