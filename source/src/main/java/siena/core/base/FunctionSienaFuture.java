package siena.core.base;

import siena.core.async.SienaFuture;

import com.google.common.base.Function;

public class FunctionSienaFuture<F, T> implements SienaFuture<T>
{
	private final Function<F, T>	mTransform;
	private F						mOrig;

	public FunctionSienaFuture(Function<F, T> pTransform, F pOrig)
	{
		mTransform = pTransform;
		mOrig = pOrig;
	}

	/**
	 * @see siena.core.async.SienaFuture#get()
	 */
	@Override
	public T get()
	{
		return mTransform.apply(mOrig);
	}

}
