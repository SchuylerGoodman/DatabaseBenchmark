package dbtesting;

/**
 * Hello world!
 *
 */
public class App
{
	static int SMALL_COUNT = 1000;
	static int LARGE_COUNT = 100;

    public static void main( String[] args )
    {
		try {

			DataGenerator gen = new StaticGenerator();

			Test t = new SQLTest();
			t.init();
			System.out.println("Creating Collection");
			t.createCollection("a");

			for (int i = 0; i < SMALL_COUNT; i++) {
				t.createDocument("a", i, gen.getSmallData(), gen.getSmallData(), gen.getSmallData());
			}

			for (int i = 0; i < SMALL_COUNT; i++) {
				t.updateDocument("a", i, gen.getSmallData(), gen.getSmallData(), gen.getSmallData());
			}

			for (int i = 0; i < SMALL_COUNT; i++) {
				t.deleteDocument("a", i);
			}


			for (int i = 0; i < LARGE_COUNT; i++) {
				t.createDocument("a", i, gen.getLargeData(), gen.getLargeData(), gen.getLargeData());
			}

			for (int i = 0; i < LARGE_COUNT; i++) {
				t.updateDocument("a", i, gen.getLargeData(), gen.getLargeData(), gen.getLargeData());
			}

			for (int i = 0; i < LARGE_COUNT; i++) {
				t.deleteDocument("a", i);
			}

			//System.in.read();
			System.out.println("Deleting Collection");
			t.deleteCollection("a");

		} catch (Exception e) {
			e.printStackTrace();
		}
    }
}
