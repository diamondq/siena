package siena.base.test;

import java.util.List;

import siena.PersistenceManager;

public class GenericMassTest extends BaseMassTest
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
	public void tearDown() throws Exception
	{
		super.tearDown();
		GenericTestSetup.setup.tearDown(this);
	}

}
