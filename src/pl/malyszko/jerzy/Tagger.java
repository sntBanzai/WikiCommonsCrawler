package pl.malyszko.jerzy;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Tagger {

	public static void main(String[] args) throws FileNotFoundException, IOException {
		File file = new File(args[0]);
		Set<String> set = Files.lines(file.toPath(), Charset.forName("UTF-8")).parallel().map(Tagger::apply)
				.sequential().collect(Collectors.toSet());
		File result = new File(file.getAbsolutePath().replace("output", "tagged"));
		Files.write(result.toPath(), set, Charset.forName("UTF-8"));
	}

	static Set<String> redundantWords = Collections.unmodifiableSet(Stream
			.of("in", "by", "with", "the", "of", "paintings", "frescoes", "painting", "fresco", "paintings_by_style",
					"style", "artist", "works", "subject", "genre", "painters", "gallery", "and", "art","a","as")
			.collect(Collectors.toSet()));

	public static String apply(String line) {
		String[] split = line.split("@@");
		StringBuilder sb = new StringBuilder(split[0]);
		String[] split2 = split[1].split("@");
		Set<String> tags = new HashSet<>();
		for (String category : split2) {
			Stream.of(category.split(" ")).map(Tagger::normalize).filter(s -> !redundantWords.contains(s.toLowerCase()))
					.collect(Collectors.toSet()).forEach(tag -> tags.add(tag));
		}
		sb.append("@").append(Arrays.deepToString(tags.toArray()));
		return sb.toString();
	}

	private static String normalize(String rawTag) {
		String replace = rawTag.toLowerCase().trim().replace(",", "").replace(".", "").replace("(", "").replace(")",
				"");
		if (replace.startsWith("-")) {
			replace = replace.substring(1);
		}
		if (replace.endsWith("-")) {
			replace = replace.substring(0, replace.length() - 1);
		}
		return replace;
	}

}
