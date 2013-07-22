package siena.core.base;

import siena.SienaException;
import siena.core.async.SienaFuture;

import com.google.common.base.Function;

public class CheckFuture<T> implements Function<SienaFuture<T>, SienaFuture<T>>
{
	private final T			mValue;
	private final String	mMessage;

	public CheckFuture(T pValue, String pMessage)
	{
		mValue = pValue;
		mMessage = pMessage;
	}

	@Override
	public SienaFuture<T> apply(SienaFuture<T> pFrom)
	{
		T value = pFrom.get();
		if (mValue == null)
		{
			if (value == null)
				return pFrom;
			else
				throw new SienaException(mMessage);
		}
		else
		{
			if (mValue.equals(value))
				return pFrom;
			else
				throw new SienaException(mMessage);
		}
	}
}
