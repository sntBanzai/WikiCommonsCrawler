package pl.malyszko.jerzy;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class OutputFileSplitter {

	static BiPredicate<String, Map<Boolean, Collection<String>>> pred = (str,
			col) -> col.get(Boolean.TRUE).stream().anyMatch(fonem -> str.contains(fonem))
					&& col.get(Boolean.FALSE).stream().noneMatch(fonem -> str.contains(fonem));

	public static void main(String[] args) throws FileNotFoundException, IOException {
		String fileLoc = args[0];
		int splitFactor = 10;
		if (args.length > 1) {
			splitFactor = Integer.parseInt(args[1]);
		}
		Set<String> seekFonems = Stream
				.of("painting", "Painting", "PAINTING", "canvas", "Canvas", "CANVAS", "brush", "portrait", "Portrait",
						"still-life", "Still-life", "PORTRAIT", "STILL-LIFE", "fresco", "Fresco", "FRESCO",
						"sketch", "Sketch", "SKETCH", "drawing", "Drawing", "DRAWING", "bild", "Bild", "BILD")
				.collect(Collectors.toSet());
		Set<String> omitFonems = Stream.of("photo", "Photo", "PHOTO").collect(Collectors.toSet());

		Map<Boolean, Collection<String>> predArg = new HashMap<>();
		predArg.put(Boolean.TRUE, seekFonems);
		predArg.put(Boolean.FALSE, omitFonems);
		File file = new File(fileLoc);
		long threshold = file.length() / splitFactor;
		try (BufferedReader reader = new BufferedReader(
				new InputStreamReader(new FileInputStream(file), Charset.forName("UTF-8")))) {
			breakpoint: for (int i = 0; i < splitFactor; i++) {
				File chunkFile = new File(fileLoc.replace(".output", "chunk" + i + ".output"));
				try (OutputStreamWriter osw = new OutputStreamWriter(new FileOutputStream(chunkFile),
						Charset.forName("UTF-8"))) {
					long currIterBuffer = 0;
					String lineRead = null;
					do {
						lineRead = reader.readLine();
						if (lineRead == null)
							break breakpoint;
						String category = lineRead.split("@@@")[1];
						if (pred.test(category, predArg)) {
							currIterBuffer += lineRead.getBytes(Charset.forName("UTF-8")).length;
							osw.write(lineRead);
							osw.write("\n");
						}
					} while (currIterBuffer < threshold);
				}
			}
		}
	}

}
