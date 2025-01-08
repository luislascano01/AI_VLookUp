package com.sabadell.ai_vlookup;

import org.yaml.snakeyaml.Yaml;
import java.io.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.io.IOException;
import java.io.File;
import java.net.URISyntaxException;

public class BidirectionalGroupMap {

	private Map<String, List<GroupBlock>> refToGroups = new HashMap<>();
	private Map<String, List<GroupBlock>> targetToGroups = new HashMap<>();

	// This maps from a list of reference blocks to a list of target blocks
	private Map<GroupBlock, List<GroupBlock>> refGroupsToTargetGroups = new HashMap<>();
	// This maps from a list of target blocks to a list of reference blocks
	private Map<GroupBlock, List<GroupBlock>> targetGroupsToRefGroups = new HashMap<>();

	public BidirectionalGroupMap(File yamlFile) throws IOException {
		// 1) Load YAML into a Map
		Yaml yaml = new Yaml();
		Map<String, Object> root;
		try (InputStream is = new FileInputStream(yamlFile)) {
			root = yaml.load(is);
		} catch (Exception e) {
			System.err.println("Error reading YAML file:\n" + e.toString());
			throw new IOException("YAML file is empty or invalid.");
		}

		// 2) Extract the top-level sections
		@SuppressWarnings("unchecked")
		Map<String, List<String>> referenceGroupsRaw = (Map<String, List<String>>) root.get("reference_groups");
		@SuppressWarnings("unchecked")
		Map<String, List<String>> targetGroupsRaw = (Map<String, List<String>>) root.get("target_groups");
		@SuppressWarnings("unchecked")
		Map<String, Object> refToTgtRaw = (Map<String, Object>) root.get("ref_to_tgt");
		@SuppressWarnings("unchecked")
		Map<String, Object> tgtToRefRaw = (Map<String, Object>) root.get("tgt_to_ref");

		// 3) Parse the reference groups into a map: groupName -> GroupBlock
		Map<String, GroupBlock> refGroupMap = parseGroupBlocksFromYaml(referenceGroupsRaw);

		// 4) Parse the target groups into a map: groupName -> GroupBlock
		Map<String, GroupBlock> targetGroupMap = parseGroupBlocksFromYaml(targetGroupsRaw);

		// 5) Build refToGroups / targetToGroups
		// (each group name -> single GroupBlock in a list)
		for (Map.Entry<String, GroupBlock> entry : refGroupMap.entrySet()) {
			refToGroups.put(entry.getKey(), Collections.singletonList(entry.getValue()));
		}
		for (Map.Entry<String, GroupBlock> entry : targetGroupMap.entrySet()) {
			targetToGroups.put(entry.getKey(), Collections.singletonList(entry.getValue()));
		}

		// 6) Parse the relationships into Map<String, List<String>>
		Map<String, List<String>> refToTgtRelationships = parseYamlRelationships(refToTgtRaw);
		Map<String, List<String>> tgtToRefRelationships = parseYamlRelationships(tgtToRefRaw);

		// 7) Build the final relationship maps using the new structure:
		// refGroupsToTargetGroups: reference GroupBlock -> list of target GroupBlock(s)
		// targetGroupsToRefGroups: target GroupBlock -> list of reference GroupBlock(s)

		// -- For "Ref->Tgt": key is a T_GROUP name, value is a list of REF group names
		// So for each T_GROUP name, we map *each* REF group block to that T_GROUP block
		for (Map.Entry<String, List<String>> entry : refToTgtRelationships.entrySet()) {
			String tGroupName = entry.getKey(); // e.g. "Invoice_Details"
			GroupBlock tBlock = targetGroupMap.get(tGroupName);
			if (tBlock == null)
				continue;

			// For each reference group name that maps to that T_GROUP
			for (String refGroupName : entry.getValue()) {
				GroupBlock rBlock = refGroupMap.get(refGroupName);
				if (rBlock == null)
					continue;

				// Insert (rBlock -> tBlock) in refGroupsToTargetGroups
				// rBlock is the key, tBlock is appended to the value list
				refGroupsToTargetGroups.computeIfAbsent(rBlock, k -> new ArrayList<>()).add(tBlock);
			}
		}

		// -- For "Tgt->Ref": key is a REF group name, value is a list of T_GROUP names
		// So for each REF group name, we map *each* T_GROUP block to that REF group
		// block
		for (Map.Entry<String, List<String>> entry : tgtToRefRelationships.entrySet()) {
			String refGroupName = entry.getKey(); // e.g. "Transactions"
			GroupBlock rBlock = refGroupMap.get(refGroupName);
			if (rBlock == null)
				continue;

			// For each target group name that references that REF group
			for (String tGroupName : entry.getValue()) {
				GroupBlock tBlock = targetGroupMap.get(tGroupName);
				if (tBlock == null)
					continue;

				// Insert (tBlock -> rBlock) in targetGroupsToRefGroups
				targetGroupsToRefGroups.computeIfAbsent(tBlock, k -> new ArrayList<>()).add(rBlock);
			}
		}
	}

	/**
	 * Given a map from "something" -> (either a String or a List<String>), produce
	 * a normalized map: "something" -> List<String>.
	 *
	 * Example: "T_Group_1": "Group_1" "T_Group_2": ["Group_1", "Group_2"] becomes {
	 * "T_Group_1" -> ["Group_1"], "T_Group_2" -> ["Group_1","Group_2"] }
	 */
	private Map<String, List<String>> parseYamlRelationships(Map<String, Object> rawMap) {
		Map<String, List<String>> result = new LinkedHashMap<>();
		if (rawMap == null) {
			return result; // or throw an exception if needed
		}

		for (Map.Entry<String, Object> entry : rawMap.entrySet()) {
			String key = entry.getKey();
			Object val = entry.getValue();

			// if "val" is a string => single item list
			// if "val" is a list => cast to List<String>
			if (val instanceof String) {
				result.put(key, Collections.singletonList((String) val));
			} else if (val instanceof List) {
				// Generically, we can do a safe cast to List<?> then map to strings
				List<?> rawList = (List<?>) val;
				List<String> strList = new ArrayList<>();
				for (Object o : rawList) {
					strList.add(o.toString());
				}
				result.put(key, strList);
			}
		}

		return result;
	}

	/**
	 * Parse a YAML map: groupName -> list of "HeaderName(weight)" strings,
	 * returning a map: groupName -> GroupBlock.
	 */
	private Map<String, GroupBlock> parseGroupBlocksFromYaml(Map<String, List<String>> groupsMap) throws IOException {
		Map<String, GroupBlock> groupMap = new HashMap<>();

		if (groupsMap == null) {
			return groupMap; // or throw an exception if needed
		}

		// We’ll reuse the same regex logic as before
		Pattern p = Pattern.compile("(\\S+)\\((\\d+(\\.\\d+)?)\\)");

		for (Map.Entry<String, List<String>> entry : groupsMap.entrySet()) {
			String groupName = entry.getKey();
			List<String> rawHeaders = entry.getValue();
			if (rawHeaders == null || rawHeaders.isEmpty())
				continue;

			List<String> headerList = new ArrayList<>();
			List<Double> weightList = new ArrayList<>();

			// Parse each "Header_1(3)" style string
			for (String spec : rawHeaders) {
				Matcher m = p.matcher(spec.trim());
				if (m.find()) {
					String headerName = m.group(1);
					Double weightValue = Double.valueOf(m.group(2));
					headerList.add(headerName);
					weightList.add(weightValue);
				}
			}

			if (!headerList.isEmpty()) {
				// Sum for overallWeight
				Double overallWeight = weightList.stream().mapToDouble(Double::doubleValue).sum();

				GroupBlock gb = new GroupBlock(groupName, headerList.toArray(new String[0]),
						weightList.toArray(new Double[0]), overallWeight);
				groupMap.put(groupName, gb);
			}
		}

		return groupMap;
	}

	/**
	 * Parses a block of lines with the format:
	 * 
	 * Group_1: Header_1(3), Header_3(2), Header_4(1); Group_2: Header_2(1),
	 * Header_4(3);
	 * 
	 * ... producing a map from groupName -> GroupBlock
	 */
	private Map<String, GroupBlock> parseGroupBlocks(String section, boolean isRef) throws IOException {
		Map<String, GroupBlock> groupMap = new HashMap<>();

		// Each line generally looks like:
		// Group_1: Header_1(3), Header_3(2), Header_4(1);
		// We'll parse these lines individually
		String[] lines = section.split("\\r?\\n");
		for (String line : lines) {
			line = line.trim();
			if (line.isEmpty())
				continue;

			// The pattern: <GroupName>: <Header_1>(<weight>), <Header_2>(<weight>)...;
			// We'll separate group name from the rest
			String[] parts = line.split(":");
			if (parts.length < 2) {
				continue; // or throw an exception if invalid
			}

			String groupName = parts[0].trim();
			String headersPart = parts[1].trim();

			// remove trailing semicolon if present
			headersPart = headersPart.replaceAll(";$", "").trim();

			// Now split by commas => each chunk is like "Header_1(3)"
			String[] headerSpecs = headersPart.split(",");
			List<String> headerList = new ArrayList<>();
			List<Double> weightList = new ArrayList<>();

			// We'll use a regex to grab everything inside: headerName(weight)
			// e.g. Header_1(3)
			Pattern p = Pattern.compile("(\\S+)\\((\\d+(\\.\\d+)?)\\)");
			for (String spec : headerSpecs) {
				spec = spec.trim();
				Matcher matcher = p.matcher(spec);
				if (matcher.find()) {
					String headerName = matcher.group(1);
					Double weightValue = Double.valueOf(matcher.group(2));
					headerList.add(headerName);
					weightList.add(weightValue);
				}
			}

			if (!headerList.isEmpty()) {
				// compute overallWeight as sum of all weights
				Double overallWeight = weightList.stream().mapToDouble(Double::doubleValue).sum();

				// create the GroupBlock
				GroupBlock gb = new GroupBlock(groupName, headerList.toArray(new String[0]),
						weightList.toArray(new Double[0]), overallWeight);
				groupMap.put(groupName, gb);
			}
		}

		return groupMap;
	}

	/**
	 * Parse lines of the form: T_Group_1: Group_1 T_Group_2: Group_2 or Group_1:
	 * T_Group_2 Group_2: T_Group_1, T_Group_2 ... producing a map: key -> list of
	 * values.
	 *
	 * Example: "T_Group_1: Group_1" => { "T_Group_1" : ["Group_1"] } "Group_2:
	 * T_Group_1, T_Group_2" => { "Group_2" : ["T_Group_1","T_Group_2"] }
	 */
	private Map<String, List<String>> parseRelationships(String section) {
		Map<String, List<String>> rels = new LinkedHashMap<>();
		String[] lines = section.split("\\r?\\n");
		for (String line : lines) {
			line = line.trim();
			if (line.isEmpty())
				continue;

			// Example line: "Group_1: T_Group_2"
			String[] parts = line.split(":");
			if (parts.length < 2) {
				continue;
			}
			String left = parts[0].trim();
			String right = parts[1].trim().replaceAll(";$", "");

			// e.g. right might be "T_Group_2" or "T_Group_1, T_Group_2"
			List<String> targets = new ArrayList<>();
			for (String val : right.split(",")) {
				val = val.trim();
				if (!val.isEmpty()) {
					targets.add(val);
				}
			}
			rels.put(left, targets);
		}
		return rels;
	}

	/**
	 * Returns a LaTeX string (using TikZ) that draws a directed graph of: 1)
	 * Reference groups (left column) and their headers (further left), 2) Target
	 * groups (right column) and their headers (further right), 3) Arrows for
	 * (header -> groupBlock), 4) Arrows for (refGroupsToTargetGroups) and
	 * (targetGrupsToRefGroups).
	 *
	 * We escape underscores in labels, and remove underscores from node IDs to
	 * prevent LaTeX "missing $" errors.
	 */
	/*******************************************************
	 * toLatexGraph with 2 enhancements: 1) Double diagram width (xscale=2) 2) Soft
	 * background-color boxes for Reference Headers (left), Groups (middle), and
	 * Target Headers (right).
	 *******************************************************/
	// We assume you have changed your data fields to:
	// private Map<GroupBlock, List<GroupBlock>> refGroupsToTargetGroups = new
	// HashMap<>();
	// private Map<GroupBlock, List<GroupBlock>> targetGroupsToRefGroups = new
	// HashMap<>();
	//
	// Instead of Map<List<GroupBlock>, List<GroupBlock>>.

	private static final int GAP_BETWEEN_GROUPS = 1; // extra row(s) after each group block
	private static final double ROW_SPACING = 1.5; // vertical distance between adjacent rows

	public String toLatexGraph() {
		// Helper to escape underscores in displayed labels
		java.util.function.Function<String, String> escapeLabel = s -> s.replace("_", "\\_");

		// Helper to remove underscores from internal TikZ node names
		java.util.function.Function<String, String> removeUnderscores = s -> s.replace("_", "");

		// ------------------------------------------------------
		// X-coordinates for groups & headers
		// ------------------------------------------------------
		double refGroupX = 0; // reference groups
		double refHeaderX = -2; // reference headers
		double tgtGroupX = 4; // target groups
		double tgtHeaderX = 6; // target headers

		// ------------------------------------------------------
		// Lists for background fit boxes
		// ------------------------------------------------------
		java.util.List<String> refHeaderIDs = new java.util.ArrayList<>();
		java.util.List<String> groupIDs = new java.util.ArrayList<>();
		java.util.List<String> tgtHeaderIDs = new java.util.ArrayList<>();

		// ------------------------------------------------------
		// LaTeX Preamble
		// ------------------------------------------------------
		StringBuilder sb = new StringBuilder();
		sb.append("\\documentclass[tikz]{standalone}\n").append("\\usepackage{tikz}\n")
				.append("\\usetikzlibrary{arrows.meta,positioning,fit,backgrounds}\n").append("\\begin{document}\n")
				.append("\\begin{tikzpicture}[\n").append("    xscale=2,\n").append("    >={Stealth},\n")
				.append("    auto,\n").append("    node distance=1.6cm,\n").append("    semithick\n").append("]\n\n");

		// ------------------------------------------------------
		// 1) Place Reference Groups (left side) + their headers
		// ------------------------------------------------------
		java.util.List<String> refNames = new java.util.ArrayList<>(refToGroups.keySet());
		java.util.Collections.sort(refNames);

		int currentRefRow = 0;

		for (String groupName : refNames) {
			java.util.List<GroupBlock> blocks = refToGroups.get(groupName);
			if (blocks == null)
				continue;

			for (GroupBlock block : blocks) {
				int numHeaders = block.getHeaders().size();
				int totalRows = numHeaders + 1; // group + headers
				int startRow = currentRefRow;
				int endRow = currentRefRow + totalRows - 1;

				double centerRow = (startRow + endRow) / 2.0;
				int centerRowInt = (int) Math.floor(centerRow + 0.5);

				// Place the reference group at the center
				String groupNodeID = "ref" + removeUnderscores.apply(block.groupName);
				String groupNodeLbl = escapeLabel.apply(block.groupName);
				double groupY = -centerRow * ROW_SPACING;

				sb.append(String.format("\\node (%s) at (%.1f, %.2f) {%s};\n", groupNodeID, refGroupX, groupY,
						groupNodeLbl));
				groupIDs.add(groupNodeID);

				// Build row positions for headers
				java.util.List<Integer> rowPositions = new java.util.ArrayList<>();
				for (int r = startRow; r <= endRow; r++) {
					rowPositions.add(r);
				}
				// remove center row (occupied by the group)
				rowPositions.remove(Integer.valueOf(centerRowInt));

				// Sort headers if desired
				java.util.List<String> headers = new java.util.ArrayList<>(block.getHeaders());
				java.util.Collections.sort(headers);

				// Place each header
				for (int hIndex = 0; hIndex < headers.size(); hIndex++) {
					String header = headers.get(hIndex);
					int headerRow = rowPositions.get(hIndex);
					double headerY = -headerRow * ROW_SPACING;

					String headerID = "hdrRef" + removeUnderscores.apply(block.groupName) + "_"
							+ removeUnderscores.apply(header);
					String headerLbl = escapeLabel.apply(header);

					sb.append(String.format("\\node (%s) at (%.1f, %.2f) {%s};\n", headerID, refHeaderX, headerY,
							headerLbl));
					refHeaderIDs.add(headerID);

					Double w = block.getHeaderWeight(header);
					String wLabel = String.format("%.2f", w);

					// Arrow (header -> group), labeled with weight
					sb.append(String.format("\\draw[->] (%s) -- node[midway,above,sloped,black]{%s} (%s);\n", headerID,
							wLabel, groupNodeID));
				}

				currentRefRow = endRow + 1 + GAP_BETWEEN_GROUPS;
				sb.append("\n");
			}
		}

		// ------------------------------------------------------
		// 2) Place Target Groups (right side) + their headers
		// ------------------------------------------------------
		java.util.List<String> tgtNames = new java.util.ArrayList<>(targetToGroups.keySet());
		java.util.Collections.sort(tgtNames);

		int currentTgtRow = 0;

		for (String groupName : tgtNames) {
			java.util.List<GroupBlock> blocks = targetToGroups.get(groupName);
			if (blocks == null)
				continue;

			for (GroupBlock block : blocks) {
				int numHeaders = block.getHeaders().size();
				int totalRows = numHeaders + 1;
				int startRow = currentTgtRow;
				int endRow = currentTgtRow + totalRows - 1;

				double centerRow = (startRow + endRow) / 2.0;
				int centerRowInt = (int) Math.floor(centerRow + 0.5);

				String groupNodeID = "tgt" + removeUnderscores.apply(block.groupName);
				String groupNodeLbl = escapeLabel.apply(block.groupName);
				double groupY = -centerRow * ROW_SPACING;

				sb.append(String.format("\\node (%s) at (%.1f, %.2f) {%s};\n", groupNodeID, tgtGroupX, groupY,
						groupNodeLbl));
				groupIDs.add(groupNodeID);

				java.util.List<Integer> rowPositions = new java.util.ArrayList<>();
				for (int r = startRow; r <= endRow; r++) {
					rowPositions.add(r);
				}
				rowPositions.remove(Integer.valueOf(centerRowInt));

				java.util.List<String> headers = new java.util.ArrayList<>(block.getHeaders());
				java.util.Collections.sort(headers);

				for (int hIndex = 0; hIndex < headers.size(); hIndex++) {
					String header = headers.get(hIndex);
					int headerRow = rowPositions.get(hIndex);
					double headerY = -headerRow * ROW_SPACING;

					String headerID = "hdrTgt" + removeUnderscores.apply(block.groupName) + "_"
							+ removeUnderscores.apply(header);
					String headerLbl = escapeLabel.apply(header);

					sb.append(String.format("\\node (%s) at (%.1f, %.2f) {%s};\n", headerID, tgtHeaderX, headerY,
							headerLbl));
					tgtHeaderIDs.add(headerID);

					Double w = block.getHeaderWeight(header);
					String wLabel = String.format("%.2f", w);

					// Arrow (header -> group) labeled with weight
					sb.append(String.format("\\draw[->] (%s) -- node[midway,above,sloped,black]{%s} (%s);\n", headerID,
							wLabel, groupNodeID));
				}

				currentTgtRow = endRow + 1 + GAP_BETWEEN_GROUPS;
				sb.append("\n");
			}
		}

		// ------------------------------------------------------
		// 3) Draw group->group edges using the *new* structure:
		// refGroupsToTargetGroups: (GroupBlock -> List<GroupBlock>)
		// targetGroupsToRefGroups: (GroupBlock -> List<GroupBlock>)
		// ------------------------------------------------------

		// -- Ref->Tgt edges --
		sb.append("\n% -- Ref->Tgt edges --\n");
		for (Map.Entry<GroupBlock, List<GroupBlock>> entry : refGroupsToTargetGroups.entrySet()) {
			GroupBlock refBlock = entry.getKey();
			List<GroupBlock> tgtList = entry.getValue();

			// Build node IDs
			String refNodeID = "ref" + removeUnderscores.apply(refBlock.groupName);

			for (GroupBlock tgtBlock : tgtList) {
				String tgtNodeID = "tgt" + removeUnderscores.apply(tgtBlock.groupName);
				sb.append(String.format("\\draw[->] (%s) -- (%s);\n", refNodeID, tgtNodeID));
			}
		}

		// -- Tgt->Ref edges --
		sb.append("\n% -- Tgt->Ref edges --\n");
		for (Map.Entry<GroupBlock, List<GroupBlock>> entry : targetGroupsToRefGroups.entrySet()) {
			GroupBlock tgtBlock = entry.getKey();
			List<GroupBlock> refList = entry.getValue();

			String tgtNodeID = "tgt" + removeUnderscores.apply(tgtBlock.groupName);

			for (GroupBlock refBlock : refList) {
				String refNodeID = "ref" + removeUnderscores.apply(refBlock.groupName);
				sb.append(String.format("\\draw[->] (%s) -- (%s);\n", tgtNodeID, refNodeID));
			}
		}

		// ------------------------------------------------------
		// 4) Background "fit" boxes
		// ------------------------------------------------------
		sb.append("\n% -- Background boxes for the three sections --\n");
		// Reference headers
		if (!refHeaderIDs.isEmpty()) {
			sb.append("\\begin{scope}[on background layer]\n").append("  \\node[fill=blue!10,rounded corners,")
					.append("        label=above:{Reference Headers},").append("        fit=");
			sb.append(" (").append(String.join(")(", refHeaderIDs)).append(")");
			sb.append("] {};\n\\end{scope}\n\n");
		}
		// Groups
		if (!groupIDs.isEmpty()) {
			sb.append("\\begin{scope}[on background layer]\n").append("  \\node[fill=green!10,rounded corners,")
					.append("        label=above:{Groups},").append("        fit=");
			sb.append(" (").append(String.join(")(", groupIDs)).append(")");
			sb.append("] {};\n\\end{scope}\n\n");
		}
		// Target headers
		if (!tgtHeaderIDs.isEmpty()) {
			sb.append("\\begin{scope}[on background layer]\n").append("  \\node[fill=red!10,rounded corners,")
					.append("        label=above:{Target Headers},").append("        fit=");
			sb.append(" (").append(String.join(")(", tgtHeaderIDs)).append(")");
			sb.append("] {};\n\\end{scope}\n\n");
		}

		// ------------------------------------------------------
		// 5) Finish
		// ------------------------------------------------------
		sb.append("\\end{tikzpicture}\n").append("\\end{document}\n");

		return sb.toString();
	}


	
	public static void main(String[] args) throws Exception {
		// 1) Get the resource URL for "header_configuration.yaml"
		// Make sure the file is actually in src/main/resources.
		// The leading "/" means "look in the root of the resources folder."
		java.net.URL yamlUrl = BidirectionalGroupMap.class.getResource("/header_configuration_moc.yaml");
		if (yamlUrl == null) {
			throw new IllegalStateException("Could not find 'header_configuration.yaml' in resources folder!");
		}

		// 2) Convert that URL into a File object
		File yamlFile;
		try {
			yamlFile = new File(yamlUrl.toURI());
		} catch (URISyntaxException e) {
			// If something is wrong with the URI, fallback to constructing via getPath()
			yamlFile = new File(yamlUrl.getPath());
		}

		// 3) Instantiate BidirectionalGroupMap using the alternate YAML-based
		// constructor
		BidirectionalGroupMap map = new BidirectionalGroupMap(yamlFile);

		// 4) Generate the LaTeX graph code
		String latexCode = map.toLatexGraph();

		// 5) Print it out (or save to a file, etc.)
		System.out.println(latexCode);

		// Optional: If you want to write it to a .tex file:
		// java.nio.file.Files.write(java.nio.file.Paths.get("my_diagram.tex"),
		// latexCode.getBytes());
	}

}