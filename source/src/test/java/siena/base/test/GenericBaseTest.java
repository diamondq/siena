package siena.base.test;

import java.util.List;

import siena.PersistenceManager;

public class GenericBaseTest extends BaseTest
{

	@Override
	protected void setUp() throws Exception
	{
		GenericTestSetup.confirmSetup();
		super.setUp();
	}

	@Override
	public PersistenceManager createPersistenceManager(List<Class<?>> classes) throws Exception
	{
		return GenericTestSetup.setup.createPersistenceManager(this, classes);
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
