package siena.core.async;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Directly inspired by SimpleFutureWrapper class from objectify project
 * 
 * @link 
 *       http://code.google.com/p/objectify-appengine/source/browse/trunk/src/com
 *       /googlecode/objectify/util/SimpleFutureWrapper.java
 * 
 * @author mandubian <pascal.voitot@mandubian.org>
 */
abstract public class SienaFutureWrapper<K, V> implements Future<V> {
	private Future<K> mParent;
	private final Lock lock = new ReentrantLock();
	private boolean hasResult = false;
	private V mCachedResult = null;
	private ExecutionException mCachedException = null;

	public SienaFutureWrapper(Future<K> base) {
		mParent = base;
	}

	@Override
	public boolean cancel(boolean mayInterruptIfRunning) {
		return mParent.cancel(mayInterruptIfRunning);
	}

	@Override
	public boolean isCancelled() {
		return mParent.isCancelled();
	}

	@Override
	public boolean isDone() {
		return mParent.isDone();
	}

	/**
	 * Override this method if you want to suppress an exception thrown by the
	 * parent and return a value instead.
	 */
	protected V absorbParentException(Throwable cause) throws Throwable {
		throw cause;
	}

	private V setCached(V result, Throwable ex) throws ExecutionException {
		hasResult = true;
		mCachedResult = result;
		if (ex != null) {
			if (ex instanceof ExecutionException)
				mCachedException = (ExecutionException) ex;
			else
				mCachedException = new ExecutionException(ex);
			throw mCachedException;
		}
		return mCachedResult;
	}

	@Override
	public V get() throws InterruptedException, ExecutionException {
		lock.lock();
		try {
			if (hasResult) {
				if (mCachedException != null)
					throw mCachedException;
				return mCachedResult;
			}

			try {
				K value;
				try {
					value = mParent.get();
				} catch (ExecutionException ex) {
					return setCached(absorbParentException(ex.getCause()), null);
				}
				return setCached(wrap(value), null);
			} catch (InterruptedException ex) {
				throw ex;
			} catch (Throwable ex) {
				return setCached(null, convertException(ex));
			}
		} finally {
			lock.unlock();
		}
	}

	@Override
	public V get(long timeout, TimeUnit unit) throws InterruptedException,
			TimeoutException, ExecutionException {
		long tryLockStart = System.currentTimeMillis();
		if (!lock.tryLock(timeout, unit)) {
			throw new TimeoutException();
		}
		try {
			if (hasResult) {
				if (mCachedException != null)
					throw mCachedException;
				return mCachedResult;
			}
			long remainingDeadline = TimeUnit.MILLISECONDS.convert(timeout,
					unit) - (System.currentTimeMillis() - tryLockStart);
			try {
				K value;
				try {
					value = mParent.get(remainingDeadline,
							TimeUnit.MILLISECONDS);
				} catch (ExecutionException ex) {
					return setCached(absorbParentException(ex.getCause()), null);
				}
				return setCached(wrap(value), null);
			} catch (InterruptedException ex) {
				throw ex;
			} catch (TimeoutException ex) {
				throw ex;
			} catch (Throwable ex) {
				return setCached(null, convertException(ex));
			}
		} finally {
			lock.unlock();
		}
	}

	protected Throwable convertException(Throwable cause) {
		return cause;
	}

	@SuppressWarnings("unchecked")
	protected V wrap(K value) throws Exception {
		return (V) value;
	}

}
