/*
 * Copyright 2008 Alberto Gimeno <gimenete at gmail.com>
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package siena;

import java.io.IOException;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import siena.core.async.PersistenceManagerAsync;
import siena.logging.SienaLogger;
import siena.logging.SienaLoggerFactory;

/**
 * This is the class responsible of loading
 * <code>PersistenceManager</code> implementations.
 * Each group of persistent classes must be in the same
 * package. That package must have a <code>siena.properties</code>
 * file containing the information of the <code>PersistenceManager</code>
 * to be used.
 *
 * @author gimenete
 *
 */
public class PersistenceManagerFactory {

	private static PersistenceManagerFactory singleton;
	
	/**
	 * Logger for PersistenceManagerFactory.
	 */
	protected static SienaLogger logger = SienaLoggerFactory.getLogger(PersistenceManagerFactory.class);

	private Map<String, PersistenceManager> configuration;
	private Map<String, PersistenceManagerAsync> asyncConfiguration;

	public PersistenceManagerFactory() {
		configuration = new ConcurrentHashMap<String, PersistenceManager>();
		asyncConfiguration = new ConcurrentHashMap<String, PersistenceManagerAsync>();
	}

	public static PersistenceManager getPersistenceManager(Class<?> clazz) {
		return getInstance().get(clazz);
	}

	public static PersistenceManagerAsync getPersistenceManagerAsync(Class<?> clazz) {
		return getInstance().getAsync(clazz);
	}
	
	private static PersistenceManagerFactory getInstance() {
		if(singleton == null)
			singleton = new PersistenceManagerFactory();
		return singleton;
	}

	private Properties buildProperties(String pack, Class<?> clazz) {
		URL url = clazz.getResource("siena.properties");
		if(url == null)
			throw new SienaException("Cannot load siena.properties file for package: "+pack);

		Properties p = new Properties();
		try {
			p.load(url.openStream());
		} catch (IOException e) {
			throw new SienaException(e);
		}
		// Override with system properties
		String prefix = "siena."+pack+".";
		Properties sysprops = System.getProperties();
		for (Map.Entry<Object, Object> entry : sysprops.entrySet()) {
			String key = entry.getKey().toString();
			if(key.startsWith(prefix)) {
				String value = entry.getValue().toString();
				p.setProperty(key.substring(prefix.length()), value);
			}
		}
		return p;
	}
	
	private PersistenceManager get(Class<?> clazz) {
		String pack = getPackage(clazz);
		PersistenceManager pm = configuration.get(pack);
		if(pm != null) return pm;
		
		Properties p = buildProperties(pack, clazz);
		String impl = p.getProperty("implementation");
		if(impl == null)
			throw new SienaException("key 'implementation' not found for "+pack);

		try {
			pm = (PersistenceManager) Class.forName(impl).newInstance();
			pm.init(p);
		} catch (Exception e) {
			throw new SienaException("Error while creating instance of: "+impl, e);
		}
		configuration.put(pack, pm);
		return pm;
	}


	private PersistenceManagerAsync getAsync(Class<?> clazz) {
		String pack = getPackage(clazz);
		PersistenceManagerAsync pm = asyncConfiguration.get(pack);
		if(pm != null) return pm;
		
		Properties p = buildProperties(pack, clazz);
		String impl = p.getProperty("asyncimplementation");
		if(impl == null)
			throw new SienaException("key 'asyncimplementation' not found for "+pack);

		try {
			pm = (PersistenceManagerAsync) Class.forName(impl).newInstance();
			pm.init(p);
		} catch (Exception e) {
			throw new SienaException("Error while creating instance of: "+impl, e);
		}
		asyncConfiguration.put(pack, pm);
		return pm;
	}

	public static void install(PersistenceManager pm, Class<?> clazz) {
		// if class is abstract, it is not installed
		if(!Modifier.isAbstract(clazz.getModifiers())){
			getInstance().put(pm, getPackage(clazz));
		}
	}

	public static void install(PersistenceManager pm, Iterable<Class<?>> clazzes) {
		for(Class<?> clazz: clazzes){
			if(!Modifier.isAbstract(clazz.getModifiers())){
				getInstance().put(pm, getPackage(clazz));
			}
		}
	}
	
	public static void install(PersistenceManagerAsync pm, Class<?> clazz) {
		// if class is abstract, it is not installed
		if(!Modifier.isAbstract(clazz.getModifiers())){
			getInstance().put(pm, getPackage(clazz));
		}
	}

	public static void install(PersistenceManagerAsync pm, Iterable<Class<?>> clazzes) {
		for(Class<?> clazz: clazzes){
			if(!Modifier.isAbstract(clazz.getModifiers())){
				getInstance().put(pm, getPackage(clazz));
			}
		}
	}
	
	private void put(PersistenceManager pm, String pack) {
		configuration.put(pack, pm);
	}

	private void put(PersistenceManagerAsync pm, String pack) {
		asyncConfiguration.put(pack, pm);
	}

	private static String getPackage(Class<?> clazz) {
		String clazzName = clazz.getName();
		return clazzName.substring(0, clazzName.lastIndexOf('.'));
	}

}
