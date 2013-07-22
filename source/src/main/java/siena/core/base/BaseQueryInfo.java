package siena.core.base;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

import siena.BaseQueryData;
import siena.QueryFilterSearch;
import siena.core.base.BasePersistenceManager.SearchTerm;

public class BaseQueryInfo<T, EXTCLASSINFO extends ExtClassInfo>
{
	public final BaseQueryData<T>								mQuery;
	public final EXTCLASSINFO							mCLInfo;
	public final boolean								mKeysOnly;
	public final Map<QueryFilterSearch, SearchTerm[]>	mSearchMap;

	public BaseQueryInfo(EXTCLASSINFO pCLInfo, BaseQueryData<T> pQuery, boolean pKeysOnly)
	{
		mCLInfo = pCLInfo;
		mQuery = pQuery;
		mKeysOnly = pKeysOnly;

		/* Expand any QueryFilterSearch */
		List<QueryFilterSearch> searches = pQuery.getSearches();
		if (searches.isEmpty() == false)
		{
			mSearchMap = new HashMap<QueryFilterSearch, BasePersistenceManager.SearchTerm[]>();
			for (QueryFilterSearch qfs : searches)
			{
				ArrayList<SearchTerm> list = new ArrayList<SearchTerm>();
				for (Enumeration<Object> e = new StringTokenizer(qfs.match, " ", false); e.hasMoreElements();)
				{
					SearchTerm term = new SearchTerm();
					String orig = (String) e.nextElement();
					boolean hasWilds;

					if (orig.indexOf('*') != -1)
						hasWilds = true;
					else if (orig.indexOf('%') != -1)
						hasWilds = true;
					else
						hasWilds = false;

					if (hasWilds == true)
					{
						/* Convert all the wildcards to an regular character */

						orig = orig.replace("*", "APATTERNMATCHMARKERZ");
						orig = orig.replace("%", "APATTERNMATCHMARKERZ");

						/* Escape the string so that there are no special characters */

						orig = orig.replace("\\", "\\\\");
						orig = orig.replace("[", "\\x5B");
						orig = orig.replace("]", "\\x5D");
						orig = orig.replace(".", "\\x2E");
						orig = orig.replace("^", "\\x5E");
						orig = orig.replace("$", "\\x24");
						orig = orig.replace("?", "\\x3F");
						orig = orig.replace("*", "\\x2A");
						orig = orig.replace("+", "\\x2B");
						orig = orig.replace("{", "\\x7B");
						orig = orig.replace("}", "\\x7D");
						orig = orig.replace("|", "\\x7C");
						orig = orig.replace("(", "\\x28");
						orig = orig.replace(")", "\\x29");

						/* Now replace back all the wildcards with the appropriate regular expression pattern */

						int o = -1;
						while ((o = orig.indexOf("APATTERNMATCHMARKERZ", o + 1)) != -1)
							orig = orig.substring(0, o) + ".*" + orig.substring(o + 20);

						/* Finally convert it into a Pattern */

						term.pattern = Pattern.compile(orig);
					}
					else
						term.term = orig;
					list.add(term);
				}
				mSearchMap.put(qfs, list.toArray(new SearchTerm[list.size()]));
			}
		}
		else
			mSearchMap = null;
	}
}
