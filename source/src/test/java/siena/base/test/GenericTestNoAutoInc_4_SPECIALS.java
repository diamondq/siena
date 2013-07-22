package siena.base.test;

import java.util.List;

import siena.PersistenceManager;

public class GenericTestNoAutoInc_4_SPECIALS extends BaseTestNoAutoInc_4_SPECIALS
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
	public void init()
	{
	}

	@Override
	public boolean supportsDeleteException()
	{
		return GenericTestSetup.setup.supportsDeleteException();
	}

	@Override
	public boolean supportsSearchStart()
	{
		return GenericTestSetup.setup.supportsSearchStart();
	}

	@Override
	public boolean supportsSearchEnd()
	{
		return GenericTestSetup.setup.supportsSearchEnd();
	}

	@Override
	public boolean supportsTransaction()
	{
		return GenericTestSetup.setup.supportsTransaction();
	}

	@Override
	public boolean supportsListStore()
	{
		return GenericTestSetup.setup.supportsListStore();
	}

}
