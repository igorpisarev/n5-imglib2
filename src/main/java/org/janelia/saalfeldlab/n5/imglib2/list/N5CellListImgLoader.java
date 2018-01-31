package org.janelia.saalfeldlab.n5.imglib2.list;

import java.io.IOException;
import java.util.Arrays;

import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5Reader;

import net.imglib2.Cursor;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.cache.img.list.CellListImgLoader;
import net.imglib2.cache.img.list.SingleCellListImg;
import net.imglib2.img.list.access.container.AbstractList;
import net.imglib2.view.Views;

public class N5CellListImgLoader< T, A extends AbstractList< T, A > > implements CellListImgLoader< T, A >
{
	private final N5Reader n5;

	private final String dataset;

	private final int[] cellDimensions;

	private final DatasetAttributes attributes;

	public N5CellListImgLoader( final N5Reader n5, final String dataset, final int[] cellDimensions ) throws IOException
	{
		this.n5 = n5;
		this.dataset = dataset;
		this.cellDimensions = cellDimensions;
		this.attributes = n5.getDatasetAttributes( dataset );

		if ( !DataType.SERIALIZABLE.equals( attributes.getDataType() ) )
			throw new RuntimeException( "Only for serializable types" );

		if ( ! Arrays.equals( this.cellDimensions, attributes.getBlockSize() ) )
			throw new RuntimeException( "Cell dimensions inconsistent! " + " " + Arrays.toString( cellDimensions ) + " " + Arrays.toString( attributes.getBlockSize() ) );
	}

	@Override
	public void load( final SingleCellListImg< T, A > cell )
	{
		final long[] gridPosition = new long[ cell.numDimensions() ];
		for ( int d = 0; d < gridPosition.length; ++d )
			gridPosition[ d ] = cell.min( d ) / cellDimensions[ d ];
		final DataBlock< ? > block;
		try
		{
			block = n5.readBlock( dataset, attributes, gridPosition );
		}
		catch ( final IOException e )
		{
			throw new RuntimeException( e );
		}

		if ( block != null )
			createCopy( cell, block );
	}

	public static < T > void burnIn( final RandomAccessibleInterval< T > source, final DataBlock< ? > block )
	{
		@SuppressWarnings("unchecked")
		final T[] blockData = ( T[] ) block.getData();
		final Cursor< T > sourceCursor = Views.flatIterable( source ).cursor();
		int index = 0;
		while ( sourceCursor.hasNext() )
			blockData[ index++ ] = sourceCursor.next();
	}

	private void createCopy( final SingleCellListImg< T, A > cell, final DataBlock< ? > block )
	{
		@SuppressWarnings("unchecked")
		final T[] blockData = ( T[] ) block.getData();
		final A cellData = cell.getData();
		for ( int i = 0; i < blockData.length; ++i )
			cellData.setValue( i, blockData[ i ] );
	}
}
