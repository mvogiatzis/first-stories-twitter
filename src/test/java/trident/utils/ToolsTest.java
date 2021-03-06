package trident.utils;

import java.lang.reflect.Method;
import java.util.ArrayList;

import junit.framework.TestCase;
import entities.DotProduct;
import entities.NearNeighbour;
import entities.SparseVector;
import entities.Tweet;

public class ToolsTest extends TestCase{
	
	Tools tools;
	public ToolsTest(String tls){
		super(tls);
		tools = new Tools();
	}
	
	public void testRemoveLinksAndReplies(){
		String text = "Finally a good link from @tester here it is: http://skillsmatter.com/";
		String expected = "Finally a good link from  here it is:";
		assertEquals(expected, tools.removeLinksAndReplies(text));
	}
	
	public void testRemoveLinksAndRepliesOnlyLinksAndMentions(){
		String text = "@tester   http://skillsmatter.com/";
		String expected = "";
		assertEquals(expected, tools.removeLinksAndReplies(text));
	}
	
	public void testRemoveSpecifiedString() throws Exception{
		Method m = Tools.class.getDeclaredMethod("removeSpecifiedWord", String.class, String.class);
		m.setAccessible(true);
		
		String result = (String) m.invoke(tools, "Hi @tester how is it going?", "@");
		String expected = "Hi  how is it going?";
		assertEquals(expected, result);
	}
	
	public void testComputeIntHashAllowsTest(){
		ArrayList<DotProduct> dtList = new ArrayList<DotProduct>();
		dtList.add(new DotProduct(1.0,0));
		dtList.add(new DotProduct(0.5,1));
		int result = tools.computeIntHashAllowsTest(dtList);
		assertEquals(3, result);
	}
	
	public void testComputeCosineSimilarity(){
		Tweet t = new Tweet(1L);
		double[] sparse1 = {1.0, 0.5};
		t.setSparseVector(new SparseVector(sparse1));
		
		Tweet t2 = new Tweet(2L);
		double[] sparse2 = {0.4, 0.4, 0.0, 0.7};
		t2.setSparseVector(new SparseVector(sparse2));
		
		NearNeighbour nn = tools.computeCosineSimilarity(t, t2);
		System.out.println(nn.getCosine());
		assertEquals(0.5962847939999439, nn.getCosine());
	}
	
	
	
}
