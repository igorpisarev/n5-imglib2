package org.janelia.saalfeldlab.n5.imglib2;

import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.janelia.saalfeldlab.n5.Bzip2Compression;
import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.GzipCompression;
import org.janelia.saalfeldlab.n5.Lz4Compression;
import org.janelia.saalfeldlab.n5.N5FSWriter;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.RawCompression;
import org.janelia.saalfeldlab.n5.XzCompression;
import org.janelia.saalfeldlab.n5.imglib2.list.N5SerializableUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import net.imglib2.RandomAccessibleInterval;
import net.imglib2.img.list.ListImg;
import net.imglib2.util.Pair;
import net.imglib2.view.Views;

public class N5SerializableUtilsTest
{
	private static class Item implements Serializable
	{
		private static final long serialVersionUID = -5954864712516585761L;

		public final short val;
		public final int pos;

		public Item( final short val, final int pos )
		{
			this.val = val;
			this.pos = pos;
		}

		@Override
		public String toString()
		{
			return String.format( "%d->%d", pos, val );
		}
	}

	static private String testDirPath = System.getProperty("user.home") + "/tmp/n5-imglib2-listimg-test";

	static private String datasetName = "/test/group/dataset";

	static private long[] dimensions = new long[]{ 11, 22, 33 };

	static private int[] blockSize = new int[]{ 5, 7, 9 };

	static private List< Item > data;

	static private N5Writer n5;

	protected Compression[] getCompressions() {

		return new Compression[] {
				new RawCompression(),
				new Bzip2Compression(),
				new GzipCompression(),
				new Lz4Compression(),
				new XzCompression()
			};
	}

	/**
	 * @throws java.lang.Exception
	 */
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		final File testDir = new File(testDirPath);
		testDir.mkdirs();
		if (!(testDir.exists() && testDir.isDirectory()))
			throw new IOException("Could not create test directory for HDF5Utils test.");

		n5 = new N5FSWriter(testDirPath);

		final Random rnd = new Random();

		final int numElements = ( int )( dimensions[ 0 ] * dimensions[ 1 ] * dimensions[ 2 ] );
		data = new ArrayList<>( numElements );
		for ( int i = 0; i < numElements; ++i )
		{
			final short val = ( short ) rnd.nextInt();
			data.add( new Item( val, i ) );
		}
	}

	/**
	 * @throws java.lang.Exception
	 */
	@AfterClass
	public static void rampDownAfterClass() throws Exception {
		n5.remove("");
	}

	@Before
	public void setUp() throws Exception
	{}

	@After
	public void tearDown() throws Exception
	{}

	@Test
	public void testSaveAndOpen() throws InterruptedException, ExecutionException
	{
		for ( final Compression compression : getCompressions() )
		{
			System.out.println( "Testing n5-imglib2 using serializable type with compression=" + compression.getType() );
			final ListImg< Item > listImg = new ListImg<>( data, dimensions );
			try
			{
				N5SerializableUtils.save( listImg, n5, datasetName, blockSize, compression );
				RandomAccessibleInterval< Item > loaded = N5SerializableUtils.open( n5, datasetName );
				for ( final Pair< Item, Item > pair : Views.flatIterable( Views.interval( Views.pair( listImg, loaded ), listImg ) ) )
					Assert.assertEquals( pair.getA().toString(), pair.getB().toString() );
				Assert.assertTrue( n5.remove( datasetName ) );

				final ExecutorService exec = Executors.newFixedThreadPool( 4 );
				N5SerializableUtils.save( listImg, n5, datasetName, blockSize, compression, exec );
				loaded = N5SerializableUtils.open( n5, datasetName );
				for ( final Pair< Item, Item > pair : Views.flatIterable( Views.interval( Views.pair( listImg, loaded ), listImg ) ) )
					Assert.assertEquals( pair.getA().toString(), pair.getB().toString() );
				Assert.assertTrue( n5.remove( datasetName ) );
				exec.shutdown();
			}
			catch ( final IOException e )
			{
				fail("Failed by I/O exception.");
				e.printStackTrace();
			}
		}
	}
}
