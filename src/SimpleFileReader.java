import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public class SimpleFileReader {
	private BufferedReader b;
	private String filename;

	public SimpleFileReader(String filename) throws FileNotFoundException {
		this.b = new BufferedReader(new java.io.FileReader(filename));
		this.filename = filename;
	}

	public String nextLine() throws IOException {
		return b.readLine();
	}

	public String[] allLines() throws IOException {
		List<String> l = new ArrayList<>();
		String s;
		while ((s = nextLine()) != null) {
			l.add(s);
		}
		return l.toArray(new String[0]);
	}

	public Stream<String> stream() throws IOException {
		Stream<String> ret = Stream.of(allLines());
		this.close();
		this.b = new BufferedReader(new java.io.FileReader(filename));
		return ret;
	}

	public void close() throws IOException {
		b.close();
	}
}
