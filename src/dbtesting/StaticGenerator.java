
package dbtesting;

/**
 *
 * @author thomas
 */
public class StaticGenerator implements DataGenerator {

	byte[] small;
	byte[] large;

	public StaticGenerator()
	{
		small = new byte[1000];
		large = new byte[1000000];
		for (int i = 0; i < small.length; i++) {
			small[i] = (byte)(Math.random() * 255);
		}
		for (int i = 0; i < large.length; i++) {
			large[i] = (byte)(Math.random() * 255);
		}
	}

	public byte[] getSmallData()
	{
		return small;
	}

	public byte[] getLargeData()
	{
		return large;
	}

}
