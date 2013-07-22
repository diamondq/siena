package siena.base.test;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({GenericBaseTest.class, GenericMultiThreadTest.class, GenericEmbeddedTest.class,
		GenericMassTest.class, GenericModelTest.class, GenericRelatedTest.class, GenericTestNoAutoInc_1_CRUD.class,
		GenericTestNoAutoInc_2_FETCH.class, GenericTestNoAutoInc_3_ITER.class, GenericTestNoAutoInc_4_SPECIALS.class,
		GenericTestNoAutoInc_5_PAGINATE.class, GenericTestNoAutoInc_6_FETCH_ITER.class,
		GenericTestNoAutoInc_7_BATCH.class, GenericTestNoAutoInc_8_SEARCH.class,
		GenericTestNoAutoInc_9_FETCH_ITER_PAGINATE.class, GenericTestNoAutoInc_10_TRANSACTION.class,
		GenericAggregatedTest.class, GenericAsyncTest.class})
public class GenericSuite
{

}
