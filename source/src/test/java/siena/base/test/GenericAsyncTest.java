package siena.base.test;

import java.util.List;

import siena.core.async.PersistenceManagerAsync;

public class GenericAsyncTest extends BaseAsyncTest
{

	@Override
	protected void setUp() throws Exception
	{
		GenericTestSetup.confirmSetup();
		super.setUp();
	}

	@Override
	public PersistenceManagerAsync createPersistenceManager(List<Class<?>> classes) throws Exception
	{
		return GenericTestSetup.setup.createPersistenceManagerAsync(this, classes);
	}

	@Override
	protected void tearDown() throws Exception
	{
		super.tearDown();
		GenericTestSetup.setup.tearDown(this);
	}

	@Override
	public boolean supportsAutoincrement()
	{
		return GenericTestSetup.setup.supportsAutoincrement();
	}

	@Override
	public boolean supportsMultipleKeys()
	{
		return GenericTestSetup.setup.supportsMultipleKeys();
	}

	@Override
	public boolean mustFilterToOrder()
	{
		return GenericTestSetup.setup.mustFilterToOrder();
	}

}
