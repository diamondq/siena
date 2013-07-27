package siena.base.test;

import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import junit.framework.TestCase;
import siena.Model;
import siena.PersistenceManager;
import siena.PersistenceManagerFactory;
import siena.base.test.model.PersonLongAutoIDModel;
import siena.core.batch.Batch;

public abstract class BaseMultiThreadTest extends TestCase {
	static final private Logger logger = Logger.getLogger(BaseMultiThreadTest.class.getName());
	protected PersistenceManager pm;

	public abstract PersistenceManager createPersistenceManager(List<Class<?>> classes) throws Exception;
	
	private static PersonLongAutoIDModel PERSON_LONGAUTOID_TESLA = new PersonLongAutoIDModel("Nikola", "Tesla", "Smiljam", 1);		
	private static PersonLongAutoIDModel PERSON_LONGAUTOID_CURIE = new PersonLongAutoIDModel("Marie", "Curie", "Warsaw", 2);
	private static PersonLongAutoIDModel PERSON_LONGAUTOID_EINSTEIN = new PersonLongAutoIDModel("Albert", "Einstein", "Ulm", 3);
	
	@Override
	protected void setUp() throws Exception {
		super.setUp();
		List<Class<?>> classes = new ArrayList<Class<?>>();
		classes.add(PersonLongAutoIDModel.class);

		pm = createPersistenceManager(classes);
		PersistenceManagerFactory.install(pm, classes);
			
		for (Class<?> clazz : classes) {
			if(!Modifier.isAbstract(clazz.getModifiers())){
				pm.createQuery(clazz).delete();			
			}
		}
		
		Batch<PersonLongAutoIDModel> batch = PersonLongAutoIDModel.batch();
		batch.insert(PERSON_LONGAUTOID_TESLA, PERSON_LONGAUTOID_CURIE, PERSON_LONGAUTOID_EINSTEIN);
	}

	public void testMultiThreadSimple() {
		int count = 0;
	    while(count < 1000) {
	        count++;
	        logger.info("Launching " + count + "-th operation");
	        final int c = count;
	        FutureTask<Object> task = new FutureTask<Object>(
	        	new Thread() {
		        	public void run() {
		                Model.getByKey(PersonLongAutoIDModel.class, PERSON_LONGAUTOID_TESLA.id);
		    	        logger.info("Executed " + c + "-th operation");
		            }
		        }, null);
	        Thread thread = new Thread(task);
	        thread.start();
	        try {
	            task.get(1l, TimeUnit.HOURS);
	        } catch (Exception e) {
	            e.printStackTrace();
	        }   
	    }
	}
	
	
	

}
