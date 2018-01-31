/**
 *
 */
package org.janelia.saalfeldlab.n5.imglib2.list;

import static net.imglib2.cache.img.AccessFlags.VOLATILE;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.N5Writer;

import net.imglib2.Interval;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.cache.Cache;
import net.imglib2.cache.img.list.CachedCellListImg;
import net.imglib2.cache.img.list.ListDataAccessFactory;
import net.imglib2.cache.img.list.LoadedCellListImgCacheLoader;
import net.imglib2.cache.ref.SoftRefLoaderCache;
import net.imglib2.img.basictypeaccess.volatiles.VolatileAccess;
import net.imglib2.img.cell.Cell;
import net.imglib2.img.cell.CellGrid;
import net.imglib2.img.cell.LazyCellImg;
import net.imglib2.img.list.access.container.AbstractList;
import net.imglib2.util.Intervals;
import net.imglib2.view.Views;

/**
 * @author Stephan Saalfeld &lt;saalfelds@janelia.hhmi.org&gt;
 *
 */
public class N5SerializableUtils {

	private N5SerializableUtils() {}

	/**
	 * Creates a {@link DataBlock} of matching type and copies the content of
	 * source into it.  This is a helper method with redundant parameters.
	 *
	 * @param source
	 * @param dataType
	 * @param intBlockSize
	 * @param longBlockSize
	 * @param gridPosition
	 * @return
	 */
	private static final DataBlock< ? > createDataBlock(
			final RandomAccessibleInterval< ? > source,
			final DataType dataType,
			final int[] intBlockSize,
			final long[] longBlockSize,
			final long[] gridPosition )
	{
		final DataBlock< ? > dataBlock = dataType.createDataBlock( intBlockSize, gridPosition );
		switch ( dataType )
		{
		case SERIALIZABLE:
			N5CellListImgLoader.burnIn( source, dataBlock );
			break;
		default:
			throw new IllegalArgumentException( "Type " + dataType.name() + " not supported!" );
		}

		return dataBlock;
	}

	/**
	 * Crops the dimensions of a {@link DataBlock} at a given offset to fit
	 * into and {@link Interval} of given dimensions.  Fills long and int
	 * version of cropped block size.  Also calculates the grid raster position
	 * assuming that the offset divisible by block size without remainder.
	 *
	 * @param max
	 * @param offset
	 * @param blockDimensions
	 * @param croppedBlockDimensions
	 * @param intCroppedBlockDimensions
	 * @param gridPosition
	 */
	private static void cropBlockDimensions(
			final long[] max,
			final long[] offset,
			final int[] blockDimensions,
			final long[] croppedBlockDimensions,
			final int[] intCroppedBlockDimensions,
			final long[] gridPosition )
	{
		for ( int d = 0; d < max.length; ++d )
		{
			croppedBlockDimensions[ d ] = Math.min( blockDimensions[ d ], max[ d ] - offset[ d ] + 1 );
			intCroppedBlockDimensions[ d ] = ( int )croppedBlockDimensions[ d ];
			gridPosition[ d ] = offset[ d ] / blockDimensions[ d ];
		}
	}

	/**
	 * Crops the dimensions of a {@link DataBlock} at a given offset to fit
	 * into and {@link Interval} of given dimensions.  Fills long and int
	 * version of cropped block size.  Also calculates the grid raster position
	 * plus a grid offset assuming that the offset divisible by block size
	 * without remainder.
	 *
	 * @param max
	 * @param offset
	 * @param gridOffset
	 * @param blockDimensions
	 * @param croppedBlockDimensions
	 * @param intCroppedBlockDimensions
	 * @param gridPosition
	 */
	private static void cropBlockDimensions(
			final long[] max,
			final long[] offset,
			final long[] gridOffset,
			final int[] blockDimensions,
			final long[] croppedBlockDimensions,
			final int[] intCroppedBlockDimensions,
			final long[] gridPosition )
	{
		for ( int d = 0; d < max.length; ++d )
		{
			croppedBlockDimensions[ d ] = Math.min( blockDimensions[ d ], max[ d ] - offset[ d ] + 1 );
			intCroppedBlockDimensions[ d ] = ( int )croppedBlockDimensions[ d ];
			gridPosition[ d ] = offset[ d ] / blockDimensions[ d ] + gridOffset[ d ];
		}
	}

	/**
	 * Open an N5 dataset as a memory cached {@link LazyCellImg}.
	 *
	 * @param n5
	 * @param dataset
	 * @return
	 * @throws IOException
	 */
	public static final < T extends Serializable, A extends AbstractList< T, A > > RandomAccessibleInterval< T > open(
			final N5Reader n5,
			final String dataset ) throws IOException
	{
		final DatasetAttributes attributes = n5.getDatasetAttributes( dataset );
		if ( !DataType.SERIALIZABLE.equals( attributes.getDataType() ) )
			throw new RuntimeException( "Works only with serializable types" );

		final long[] dimensions = attributes.getDimensions();
		final int[] blockSize = attributes.getBlockSize();
		final CellGrid grid = new CellGrid( dimensions, blockSize );

		final N5CellListImgLoader< T, A > loader = new N5CellListImgLoader<>( n5, dataset, blockSize );
		final SoftRefLoaderCache< Long, Cell< A > > cache = new SoftRefLoaderCache<>();
		final Cache< Long, Cell< A > > cacheLoader = cache.withLoader( LoadedCellListImgCacheLoader.get( grid, loader ) );
		final CachedCellListImg< T, A > img = new CachedCellListImg<>( grid, cacheLoader, ListDataAccessFactory.get() );

		return img;
	}

	/**
	 * Open an N5 dataset as a memory cached {@link LazyCellImg} using {@link VolatileAccess}.
	 *
	 * @param n5
	 * @param dataset
	 * @return
	 * @throws IOException
	 */
	public static final < T extends Serializable, A extends AbstractList< T, A > > RandomAccessibleInterval< T > openVolatile(
			final N5Reader n5,
			final String dataset ) throws IOException
	{
		final DatasetAttributes attributes = n5.getDatasetAttributes( dataset );
		if ( !DataType.SERIALIZABLE.equals( attributes.getDataType() ) )
			throw new RuntimeException( "Works only with serializable types" );

		final long[] dimensions = attributes.getDimensions();
		final int[] blockSize = attributes.getBlockSize();
		final CellGrid grid = new CellGrid( dimensions, blockSize );

		final N5CellListImgLoader< T, A > loader = new N5CellListImgLoader<>( n5, dataset, blockSize );
		final SoftRefLoaderCache< Long, Cell< A > > cache = new SoftRefLoaderCache<>();
		final Cache< Long, Cell< A > > cacheLoader = cache.withLoader( LoadedCellListImgCacheLoader.get( grid, loader, VOLATILE ) );
		final CachedCellListImg< T, A > img = new CachedCellListImg<>( grid, cacheLoader, ListDataAccessFactory.get( VOLATILE ) );

		return img;
	}

	/**
	 * Save a {@link RandomAccessibleInterval} as an N5 dataset.
	 *
	 * @param source
	 * @param n5
	 * @param dataset
	 * @param attributes
	 * @param gridOffset
	 * @throws IOException
	 */
	public static final < T extends Serializable > void saveBlock(
			RandomAccessibleInterval< T > source,
			final N5Writer n5,
			final String dataset,
			final DatasetAttributes attributes,
			final long[] gridOffset ) throws IOException
	{
		if ( !DataType.SERIALIZABLE.equals( attributes.getDataType() ) )
			throw new RuntimeException( "Works only with serializable types" );

		source = Views.zeroMin( source );
		final long[] dimensions = Intervals.dimensionsAsLongArray( source );

		final int n = dimensions.length;
		final long[] max = Intervals.maxAsLongArray( source );
		final long[] offset = new long[ n ];
		final long[] gridPosition = new long[ n ];
		final int[] blockSize = attributes.getBlockSize();
		final int[] intCroppedBlockSize = new int[ n ];
		final long[] longCroppedBlockSize = new long[ n ];
		for ( int d = 0; d < n; )
		{
			cropBlockDimensions( max, offset, gridOffset, blockSize, longCroppedBlockSize, intCroppedBlockSize, gridPosition );
			final RandomAccessibleInterval< T > sourceBlock = Views.offsetInterval( source, offset, longCroppedBlockSize );
			final DataBlock< ? > dataBlock = createDataBlock(
					sourceBlock,
					attributes.getDataType(),
					intCroppedBlockSize,
					longCroppedBlockSize,
					gridPosition );

			n5.writeBlock( dataset, attributes, dataBlock );

			for ( d = 0; d < n; ++d )
			{
				offset[ d ] += blockSize[ d ];
				if ( offset[ d ] <= max[ d ] )
					break;
				else
					offset[ d ] = 0;
			}
		}
	}

	/**
	 * Save a {@link RandomAccessibleInterval} as an N5 dataset.
	 *
	 * @param source
	 * @param n5
	 * @param dataset
	 * @param gridOffset
	 * @throws IOException
	 */
	public static final < T extends Serializable > void saveBlock(
			final RandomAccessibleInterval< T > source,
			final N5Writer n5,
			final String dataset,
			final long[] gridOffset ) throws IOException
	{
		final DatasetAttributes attributes = n5.getDatasetAttributes( dataset );
		if ( attributes != null )
		{
			saveBlock( source, n5, dataset, attributes, gridOffset );
		}
		else
		{
			throw new IOException( "Dataset " + dataset + " does not exist." );
		}
	}

	/**
	 * Save a {@link RandomAccessibleInterval} as an N5 dataset,
	 * multi-threaded.
	 *
	 * @param source
	 * @param n5
	 * @param dataset
	 * @param blockSize
	 * @param compressionType
	 * @param exec
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	public static final < T extends Serializable > void saveBlock(
			final RandomAccessibleInterval< T > source,
			final N5Writer n5,
			final String dataset,
			final long[] gridOffset,
			final ExecutorService exec ) throws IOException, InterruptedException, ExecutionException
	{
		final RandomAccessibleInterval< T > zeroMinSource = Views.zeroMin( source );
		final long[] dimensions = Intervals.dimensionsAsLongArray( zeroMinSource );
		final DatasetAttributes attributes = n5.getDatasetAttributes( dataset );
		if ( attributes != null )
		{
			if ( !DataType.SERIALIZABLE.equals( attributes.getDataType() ) )
				throw new RuntimeException( "Works only with serializable types" );

			final int n = dimensions.length;
			final long[] max = Intervals.maxAsLongArray( zeroMinSource );
			final long[] offset = new long[ n ];
			final int[] blockSize = attributes.getBlockSize();

			final ArrayList< Future< ? > > futures = new ArrayList<>();
			for ( int d = 0; d < n; )
			{
				final long[] fOffset = offset.clone();

				futures.add(
						exec.submit(
								() -> {

									final long[] gridPosition = new long[ n ];
									final int[] intCroppedBlockSize = new int[ n ];
									final long[] longCroppedBlockSize = new long[ n ];

									cropBlockDimensions( max, fOffset, gridOffset, blockSize, longCroppedBlockSize, intCroppedBlockSize, gridPosition );

									final RandomAccessibleInterval< T > sourceBlock = Views.offsetInterval( zeroMinSource, fOffset, longCroppedBlockSize );
									final DataBlock< ? > dataBlock = createDataBlock(
											sourceBlock,
											attributes.getDataType(),
											intCroppedBlockSize,
											longCroppedBlockSize,
											gridPosition );

									try
									{
										n5.writeBlock( dataset, attributes, dataBlock );
									}
									catch ( final IOException e )
									{
										e.printStackTrace();
									}
								} ) );

				for ( d = 0; d < n; ++d )
				{
					offset[ d ] += blockSize[ d ];
					if ( offset[ d ] <= max[ d ] )
						break;
					else
						offset[ d ] = 0;
				}
			}
			for ( final Future< ? > f : futures )
				f.get();
		}
		else
		{
			throw new IOException( "Dataset " + dataset + " does not exist." );
		}
	}

	/**
	 * Save a {@link RandomAccessibleInterval} as an N5 dataset.
	 *
	 * @param source
	 * @param n5
	 * @param dataset
	 * @param blockSize
	 * @param compressionType
	 * @throws IOException
	 */
	public static final < T extends Serializable > void save(
			RandomAccessibleInterval< T > source,
			final N5Writer n5,
			final String dataset,
			final int[] blockSize,
			final Compression compression) throws IOException
	{
		source = Views.zeroMin( source );
		final long[] dimensions = Intervals.dimensionsAsLongArray( source );
		final DatasetAttributes attributes = new DatasetAttributes(
				dimensions,
				blockSize,
				DataType.SERIALIZABLE,
				compression );

		n5.createDataset( dataset, attributes );

		final int n = dimensions.length;
		final long[] max = Intervals.maxAsLongArray( source );
		final long[] offset = new long[ n ];
		final long[] gridPosition = new long[ n ];
		final int[] intCroppedBlockSize = new int[ n ];
		final long[] longCroppedBlockSize = new long[ n ];
		for ( int d = 0; d < n; )
		{
			cropBlockDimensions( max, offset, blockSize, longCroppedBlockSize, intCroppedBlockSize, gridPosition );
			final RandomAccessibleInterval< T > sourceBlock = Views.offsetInterval( source, offset, longCroppedBlockSize );
			final DataBlock< ? > dataBlock = createDataBlock(
					sourceBlock,
					attributes.getDataType(),
					intCroppedBlockSize,
					longCroppedBlockSize,
					gridPosition );

			n5.writeBlock( dataset, attributes, dataBlock );

			for ( d = 0; d < n; ++d )
			{
				offset[ d ] += blockSize[ d ];
				if ( offset[ d ] <= max[ d ] )
					break;
				else
					offset[ d ] = 0;
			}
		}
	}

	/**
	 * Save a {@link RandomAccessibleInterval} as an N5 dataset,
	 * multi-threaded.
	 *
	 * @param source
	 * @param n5
	 * @param dataset
	 * @param blockSize
	 * @param compressionType
	 * @param exec
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	public static final < T extends Serializable > void save(
			final RandomAccessibleInterval< T > source,
			final N5Writer n5,
			final String dataset,
			final int[] blockSize,
			final Compression compression,
			final ExecutorService exec ) throws IOException, InterruptedException, ExecutionException
	{
		final RandomAccessibleInterval< T > zeroMinSource = Views.zeroMin( source );
		final long[] dimensions = Intervals.dimensionsAsLongArray( zeroMinSource );
		final DatasetAttributes attributes = new DatasetAttributes(
				dimensions,
				blockSize,
				DataType.SERIALIZABLE,
				compression );

		n5.createDataset( dataset, attributes );

		final int n = dimensions.length;
		final long[] max = Intervals.maxAsLongArray( zeroMinSource );
		final long[] offset = new long[ n ];

		final ArrayList< Future< ? > > futures = new ArrayList<>();
		for ( int d = 0; d < n; )
		{
			final long[] fOffset = offset.clone();

			futures.add(
					exec.submit(
							() -> {

								final long[] gridPosition = new long[ n ];
								final int[] intCroppedBlockSize = new int[ n ];
								final long[] longCroppedBlockSize = new long[ n ];

								cropBlockDimensions( max, fOffset, blockSize, longCroppedBlockSize, intCroppedBlockSize, gridPosition );

								final RandomAccessibleInterval< T > sourceBlock = Views.offsetInterval( zeroMinSource, fOffset, longCroppedBlockSize );
								final DataBlock< ? > dataBlock = createDataBlock(
										sourceBlock,
										attributes.getDataType(),
										intCroppedBlockSize,
										longCroppedBlockSize,
										gridPosition );

								try
								{
									n5.writeBlock( dataset, attributes, dataBlock );
								}
								catch ( final IOException e )
								{
									e.printStackTrace();
								}
							} ) );

			for ( d = 0; d < n; ++d )
			{
				offset[ d ] += blockSize[ d ];
				if ( offset[ d ] <= max[ d ] )
					break;
				else
					offset[ d ] = 0;
			}
		}
		for ( final Future< ? > f : futures )
			f.get();
	}
}
