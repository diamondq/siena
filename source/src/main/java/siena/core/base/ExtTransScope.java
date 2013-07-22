package siena.core.base;

import java.util.List;

import com.google.common.base.Function;

public class ExtTransScope
{
	public static class CallData<F>
	{
		public final Function<F, F>	function;
		public final F				data;

		public CallData(Function<F, F> pFunction, F pData)
		{
			function = pFunction;
			data = pData;
		}
	}

	public boolean					keepOpen;
	public TransResultType			transResult	= TransResultType.ROLLBACK;
	public List<CallData<Object>>	afterSuccess;

}
