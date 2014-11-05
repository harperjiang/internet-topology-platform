package edu.clarkson.cs.common.io;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * <code>LineCountingInputStream</code> will count the position of CR/LF it
 * encountered when reading content
 * 
 * @author Hao Jiang
 * @version 1.0
 * @since client-caida 0.1
 * 
 */
public class LineCountingInputStream extends FilterInputStream {

	public LineCountingInputStream(InputStream in) {
		super(in);
		// Always offer a line for new file
		linestarts.offer(0l);
	}

	private Queue<Long> linestarts = new LinkedBlockingQueue<Long>();

	private long offset = 0;

	@Override
	public int read(byte[] b, int off, int len) throws IOException {
		int readed = in.read(b, off, len);

		for (int i = 0; i < readed; i++) {
			offset++;
			// Follow only Unix standard
			if (b[off + i] == 0x0A)
				linestarts.offer(offset);
		}
		return readed;
	}

	public long linestart() {
		return linestarts.poll();
	}
}
