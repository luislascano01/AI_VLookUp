package com.sabadell.ai_vlookup;

import org.yaml.snakeyaml.Yaml;

import java.io.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This class loads reference and target group definitions (including headers
 * and their weights) from the "BackboneConfiguration" subsection of a YAML
 * file.
 */
public class BidirectionalGroupMap implements Serializable {
	private static final long serialVersionUID = 1L;

	private Map<String, List<GroupBlock>> refToGroups = new HashMap<String, List<GroupBlock>>();
	private Map<String, List<GroupBlock>> tgtToGroups = new HashMap<String, List<GroupBlock>>();
	private Map<String, GroupBlock> refGroups = new HashMap<String, GroupBlock>();
	private Map<String, GroupBlock> tgtGroups = new HashMap<String, GroupBlock>();
	private Map<String, List<GroupBlock>> refGroupsToTgtGroups = new HashMap<String, List<GroupBlock>>();
	private Map<String, List<GroupBlock>> tgtGroupsToRefGroups = new HashMap<String, List<GroupBlock>>();

	private String referenceKeyHeader = null;
	private String targetKeyHeader = null;

	private transient Yaml yaml;

	public String getReferenceKeyHeader() {
		return this.referenceKeyHeader;
	}

	public String getTargetKeyHeader() {
		return this.targetKeyHeader;
	}

	/**
	 * Constructor: loads the YAML file, parses the "BackboneConfiguration"
	 * subsection, and populates the data structures.
	 *
	 * @param yamlFile The YAML file containing the full configuration.
	 * @throws IOException if the YAML file is invalid or the
	 *                     "BackboneConfiguration" is missing.
	 */
	public BidirectionalGroupMap(File yamlFile) throws IOException {
		@SuppressWarnings("unchecked")
		Map<String, Object> backboneSection = loadConfiguration(yamlFile);

		// Parse the BackboneConfiguration data
		parseBackboneConfiguration(backboneSection);
	}

	/**
	 * Loads the entire YAML configuration file into a Map.
	 *
	 * @param yamlFile The YAML file to load.
	 * @return A Map representing the entire YAML configuration.
	 * @throws IOException if the YAML file cannot be read or parsed.
	 */
	private Map<String, Object> loadConfiguration(File yamlFile) throws IOException {
		yaml = new Yaml();
		try (InputStream is = new FileInputStream(yamlFile)) {
			return yaml.load(is);
		} catch (Exception e) {
			System.err.println("Error reading YAML file:\n" + e.toString());
			throw new IOException("YAML file is empty or invalid.");
		}
	}

	public List<GroupBlock> getGroupsFromSourceGroupName(String groupName, boolean leftToRight) {

		if (leftToRight) {

			return this.refGroupsToTgtGroups.get(groupName);
		}
		return this.tgtGroupsToRefGroups.get(groupName);
	}

	public Map<String, GroupBlock> getGroups(boolean reference) {

		if (reference) {
			return refGroups;
		} else
			return tgtGroups;
	}

	public List<GroupBlock> getGroupsFromHeader(String header, boolean reference) {

		if (reference) {
			return this.refToGroups.get(header);
		} else {
			return this.tgtToGroups.get(header);
		}
	}

	public List<String> getInputHeaders(boolean reference) {

		Set<String> keys = null;

		if (reference) {
			keys = this.refToGroups.keySet();
		} else {
			keys = this.tgtToGroups.keySet();
		}

		return new ArrayList<String>(keys);

	}

	/**
	 * Parses the BackboneConfiguration subsection and populates internal data
	 * structures.
	 *
	 * @param backboneConfig The "BackboneConfiguration" section of the YAML file.
	 */
	private void parseBackboneConfiguration(Map<String, Object> backboneConfig) throws IOException {
		this.referenceKeyHeader = (String) backboneConfig.get("reference_key_col");
		this.targetKeyHeader = (String) backboneConfig.get("target_key_col");
		@SuppressWarnings("unchecked")
		Map<String, List<String>> referenceGroupsRaw = (Map<String, List<String>>) backboneConfig
				.get("reference_groups");

		@SuppressWarnings("unchecked")
		Map<String, List<String>> targetGroupsRaw = (Map<String, List<String>>) backboneConfig.get("target_groups");

		@SuppressWarnings("unchecked")
		Map<String, Object> refToTgtRaw = (Map<String, Object>) backboneConfig.get("ref_to_tgt");

		@SuppressWarnings("unchecked")
		Map<String, Object> tgtToRefRaw = (Map<String, Object>) backboneConfig.get("tgt_to_ref");

		// Parse reference and target groups
		Map<String, GroupBlock> refGroupMap = parseGroupBlocksFromYaml(referenceGroupsRaw, refGroups);
		Map<String, GroupBlock> targetGroupMap = parseGroupBlocksFromYaml(targetGroupsRaw, tgtGroups);

		// Build refToGroups and tgtToGroups
		refGroupMap.values().forEach(
				gb -> gb.getHeaders().forEach(h -> refToGroups.computeIfAbsent(h, k -> new ArrayList<>()).add(gb)));
		targetGroupMap.values().forEach(
				gb -> gb.getHeaders().forEach(h -> tgtToGroups.computeIfAbsent(h, k -> new ArrayList<>()).add(gb)));

		// Parse relationships and build mappings
		Map<String, List<String>> refToTgtRelationships = parseYamlRelationships(refToTgtRaw);
		Map<String, List<String>> tgtToRefRelationships = parseYamlRelationships(tgtToRefRaw);

		structureSingleDirectionGroupsToGroups(refToTgtRelationships, refGroupsToTgtGroups, tgtGroups);
		structureSingleDirectionGroupsToGroups(tgtToRefRelationships, tgtGroupsToRefGroups, refGroups);
	}

	private Map<String, List<String>> parseYamlRelationships(Map<String, Object> rawMap) {
		Map<String, List<String>> result = new LinkedHashMap<>();
		if (rawMap == null)
			return result;

		rawMap.forEach((key, val) -> {
			if (val instanceof String) {
				result.put(key, Collections.singletonList((String) val));
			} else if (val instanceof List) {
				List<?> rawList = (List<?>) val;
				result.put(key, rawList.stream().map(Object::toString).toList());
			}
		});
		return result;
	}

	private Map<String, GroupBlock> parseGroupBlocksFromYaml(Map<String, List<String>> groupsMap,
			Map<String, GroupBlock> groupNameMap) {
		Map<String, GroupBlock> groupMap = new HashMap<>();
		if (groupsMap == null)
			return groupMap;

		Pattern p = Pattern.compile("(\\S+)\\((\\d+(\\.\\d+)?)\\)");

		groupsMap.forEach((groupName, rawHeaders) -> {
			List<String> headerList = new ArrayList<>();
			List<Double> weightList = new ArrayList<>();

			rawHeaders.forEach(spec -> {
				Matcher m = p.matcher(spec.trim());
				if (m.find()) {
					headerList.add(m.group(1));
					weightList.add(Double.valueOf(m.group(2)));
				}
			});

			if (!headerList.isEmpty()) {
				double overallWeight = weightList.stream().mapToDouble(Double::doubleValue).sum();
				GroupBlock gb = null;
				try {
					gb = new GroupBlock(groupName, headerList.toArray(new String[0]), weightList.toArray(new Double[0]),
							overallWeight);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				groupMap.put(groupName, gb);
				groupNameMap.put(groupName, gb);
			}
		});

		return groupMap;
	}

	private void structureSingleDirectionGroupsToGroups(Map<String, List<String>> rawMap,
			Map<String, List<GroupBlock>> storeMap, Map<String, GroupBlock> endNameMap) {
		rawMap.forEach((srcGroupName, endGroupNames) -> {
			List<GroupBlock> endBlocks = endGroupNames.stream().map(endNameMap::get).filter(Objects::nonNull).toList();
			storeMap.put(srcGroupName, endBlocks);
		});
	}

	/**
	 * Returns a LaTeX string (using TikZ) that draws a directed graph: - Reference
	 * groups (left) + headers (further left) - Target groups (right) + headers
	 * (further right) - Edges for (header -> group) and for
	 * (refGroupsToTargetGroups) and (targetGroupsToRefGroups).
	 */
	private static final int GAP_BETWEEN_GROUPS = 1; // extra row(s) after each group block
	private static final double ROW_SPACING = 1.5; // vertical distance between adjacent rows

	public String toLatexGraph() {
		// Helper to escape underscores in displayed labels
		java.util.function.Function<String, String> escapeLabel = s -> s.replace("_", "\\_");
		// Helper to remove underscores from internal TikZ node names
		java.util.function.Function<String, String> removeUnderscores = s -> s.replace("_", "");

		// X-coordinates for groups & headers
		double refGroupX = 0;
		double refHeaderX = -2;
		double tgtGroupX = 4;
		double tgtHeaderX = 6;

		List<String> refHeaderIDs = new ArrayList<>();
		List<String> groupIDs = new ArrayList<>();
		List<String> tgtHeaderIDs = new ArrayList<>();

		// Start building the LaTeX
		StringBuilder sb = new StringBuilder();
		sb.append("\\documentclass[tikz]{standalone}\n").append("\\usepackage{tikz}\n")
				.append("\\usetikzlibrary{arrows.meta,positioning,fit,backgrounds}\n").append("\\begin{document}\n")
				.append("\\begin{tikzpicture}[\n").append("    xscale=2,\n").append("    >={Stealth},\n")
				.append("    auto,\n").append("    node distance=1.6cm,\n").append("    semithick\n").append("]\n\n");

		// ------------------------------------------------------
		// 1) Place Reference Groups (left) + their headers
		// BUT notice: we previously used "refToGroups.keySet()" = group names.
		// Now "refToGroups" is keyed by headers. We want to preserve the old layout?
		// For the visualization, let's build them from the old approach:
		// We'll just gather them by groupBlock. Let's group them by group name:
		// Because the toLatexGraph was built with the assumption that refToGroups
		// is groupName -> ...
		// So let's gather reference group blocks from refGroupsToTargetGroups keySet.
		// That is the set of reference GroupBlocks. Then we can place them and their
		// headers.
		// ------------------------------------------------------
		// Collect all reference GroupBlocks
		Set<String> allRefBlocks = refGroups.keySet();
		// Sort them by groupName
		List<String> sortedRefBlocks = new ArrayList<String>(allRefBlocks);
		sortedRefBlocks.sort(Comparator.naturalOrder());

		int currentRefRow = 0;
		for (String groupName : sortedRefBlocks) {

			GroupBlock currGroup = refGroups.get(groupName);

			int numHeaders = currGroup.getHeaders().size();
			int totalRows = numHeaders + 1; // 1 for the group
			int startRow = currentRefRow;
			int endRow = currentRefRow + totalRows - 1;
			double centerRow = (startRow + endRow) / 2.0;
			int centerRowInt = (int) Math.floor(centerRow + 0.5);

			// Place the reference group node
			String groupNodeID = "ref" + removeUnderscores.apply(currGroup.groupName);
			String groupNodeLbl = escapeLabel.apply(currGroup.groupName);
			double groupY = -centerRow * ROW_SPACING;
			sb.append(
					String.format("\\node (%s) at (%.1f, %.2f) {%s};\n", groupNodeID, refGroupX, groupY, groupNodeLbl));
			groupIDs.add(groupNodeID);

			// Build row positions for the headers
			List<Integer> rowPositions = new ArrayList<>();
			for (int r = startRow; r <= endRow; r++) {
				rowPositions.add(r);
			}
			rowPositions.remove(Integer.valueOf(centerRowInt));

			// Sort the headers if you like:
			List<String> sortedHeaders = new ArrayList<>(currGroup.getHeaders());
			Collections.sort(sortedHeaders);

			for (int i = 0; i < sortedHeaders.size(); i++) {
				String header = sortedHeaders.get(i);
				int headerRow = rowPositions.get(i);
				double headerY = -headerRow * ROW_SPACING;

				String headerID = "hdrRef" + removeUnderscores.apply(currGroup.groupName) + "_"
						+ removeUnderscores.apply(header);
				String headerLbl = escapeLabel.apply(header);

				sb.append(
						String.format("\\node (%s) at (%.1f, %.2f) {%s};\n", headerID, refHeaderX, headerY, headerLbl));
				refHeaderIDs.add(headerID);

				Double w = currGroup.getHeaderWeight(header);
				String wLabel = String.format("%.2f", w);
				sb.append(String.format("\\draw[->] (%s) -- node[midway,above,sloped,black]{%s} (%s);\n", headerID,
						wLabel, groupNodeID));
			}
			currentRefRow = endRow + 1 + GAP_BETWEEN_GROUPS;
			sb.append("\n");
		}

		// ------------------------------------------------------
		// 2) Place Target Groups (right side) + their headers
		// We'll gather them from targetGroupsToRefGroups.keySet()
		// ------------------------------------------------------
		Set<String> allTgtBlocks = tgtGroupsToRefGroups.keySet();
		List<String> sortedTgtBlocks = new ArrayList<>(allTgtBlocks);
		sortedTgtBlocks.sort(Comparator.naturalOrder());

		int currentTgtRow = 0;
		for (String blockName : sortedTgtBlocks) {
			GroupBlock block = tgtGroups.get(blockName);
			int numHeaders = block.getHeaders().size();
			int totalRows = numHeaders + 1;
			int startRow = currentTgtRow;
			int endRow = currentTgtRow + totalRows - 1;
			double centerRow = (startRow + endRow) / 2.0;
			int centerRowInt = (int) Math.floor(centerRow + 0.5);

			String groupNodeID = "tgt" + removeUnderscores.apply(block.groupName);
			String groupNodeLbl = escapeLabel.apply(block.groupName);
			double groupY = -centerRow * ROW_SPACING;
			sb.append(
					String.format("\\node (%s) at (%.1f, %.2f) {%s};\n", groupNodeID, tgtGroupX, groupY, groupNodeLbl));
			groupIDs.add(groupNodeID);

			List<Integer> rowPositions = new ArrayList<>();
			for (int r = startRow; r <= endRow; r++) {
				rowPositions.add(r);
			}
			rowPositions.remove(Integer.valueOf(centerRowInt));

			List<String> sortedHeaders = new ArrayList<>(block.getHeaders());
			Collections.sort(sortedHeaders);

			for (int i = 0; i < sortedHeaders.size(); i++) {
				String header = sortedHeaders.get(i);
				int headerRow = rowPositions.get(i);
				double headerY = -headerRow * ROW_SPACING;

				String headerID = "hdrTgt" + removeUnderscores.apply(block.groupName) + "_"
						+ removeUnderscores.apply(header);
				String headerLbl = escapeLabel.apply(header);

				sb.append(
						String.format("\\node (%s) at (%.1f, %.2f) {%s};\n", headerID, tgtHeaderX, headerY, headerLbl));
				tgtHeaderIDs.add(headerID);

				Double w = block.getHeaderWeight(header);
				String wLabel = String.format("%.2f", w);
				sb.append(String.format("\\draw[->] (%s) -- node[midway,above,sloped,black]{%s} (%s);\n", headerID,
						wLabel, groupNodeID));
			}
			currentTgtRow = endRow + 1 + GAP_BETWEEN_GROUPS;
			sb.append("\n");
		}

		// ------------------------------------------------------
		// 3) Draw group->group edges using the new structure:
		// refGroupsToTargetGroups: (ref GroupBlock -> List<target GroupBlock>)
		// targetGroupsToRefGroups: (target GroupBlock -> List<ref GroupBlock>)
		// ------------------------------------------------------
		sb.append("\n% -- Ref->Tgt edges --\n");

		for (Map.Entry<String, List<GroupBlock>> entry : refGroupsToTgtGroups.entrySet()) {

			String refBlockName = entry.getKey();
			GroupBlock refBlock = this.refGroups.get(refBlockName);
			String refNodeID = "ref" + removeUnderscores.apply(refBlock.groupName);
			List<GroupBlock> tgtList = entry.getValue();
			if (tgtList != null) {
				for (GroupBlock tgtBlock : tgtList) {
					String tgtNodeID = "tgt" + removeUnderscores.apply(tgtBlock.groupName);
					sb.append(String.format("\\draw[->] (%s) -- (%s);\n", refNodeID, tgtNodeID));
				}
			}
		}

		sb.append("\n% -- Tgt->Ref edges --\n");
		for (Map.Entry<String, List<GroupBlock>> entry : tgtGroupsToRefGroups.entrySet()) {

			String tgtBlockName = entry.getKey();
			GroupBlock tgtBlock = this.tgtGroups.get(tgtBlockName);
			String tgtNodeID = "tgt" + removeUnderscores.apply(tgtBlock.groupName);
			List<GroupBlock> refList = entry.getValue();
			if (refList != null) {
				for (GroupBlock refBlock : refList) {
					String refNodeID = "ref" + removeUnderscores.apply(refBlock.groupName);
					sb.append(String.format("\\draw[->] (%s) -- (%s);\n", tgtNodeID, refNodeID));
				}
			}
		}

		// ------------------------------------------------------
		// 4) Background "fit" boxes
		// ------------------------------------------------------
		sb.append("\n% -- Background boxes for the three sections --\n");
		// Reference headers
		if (!refHeaderIDs.isEmpty()) {
			sb.append("\\begin{scope}[on background layer]\n").append("  \\node[fill=blue!10,rounded corners,\n")
					.append("        label=above:{Reference Headers},\n").append("        fit=");
			sb.append(" (").append(String.join(")(", refHeaderIDs)).append(")");
			sb.append("] {};\n\\end{scope}\n\n");
		}
		// Groups
		if (!groupIDs.isEmpty()) {
			sb.append("\\begin{scope}[on background layer]\n").append("  \\node[fill=green!10,rounded corners,\n")
					.append("        label=above:{Groups},\n").append("        fit=");
			sb.append(" (").append(String.join(")(", groupIDs)).append(")");
			sb.append("] {};\n\\end{scope}\n\n");
		}
		// Target headers
		if (!tgtHeaderIDs.isEmpty()) {
			sb.append("\\begin{scope}[on background layer]\n").append("  \\node[fill=red!10,rounded corners,\n")
					.append("        label=above:{Target Headers},\n").append("        fit=");
			sb.append(" (").append(String.join(")(", tgtHeaderIDs)).append(")");
			sb.append("] {};\n\\end{scope}\n\n");
		}

		// ------------------------------------------------------
		// 5) Finish
		// ------------------------------------------------------
		sb.append("\\end{tikzpicture}\n").append("\\end{document}\n");

		return sb.toString();
	}

	/**
	 * Quick test main
	 */
	public static void main(String[] args) throws Exception {
		if (args.length < 1) {
			System.out.println("Usage: java BidirectionalGroupMap <yamlFilePath>");
			return;
		}

		// 1) Retrieve the YAML file path from the command-line arguments
		String yamlFilePath = args[0];
		File yamlFile = new File(yamlFilePath);

		// 2) Validate the YAML file path
		if (!yamlFile.exists() || !yamlFile.isFile()) {
			System.err.println("Invalid YAML file path provided: " + yamlFilePath);
			return;
		}

		// 3) Instantiate BidirectionalGroupMap using the provided YAML file
		BidirectionalGroupMap map = new BidirectionalGroupMap(yamlFile);

		// 4) Generate the LaTeX graph code
		String latexCode = map.toLatexGraph();

		// 5) Print the LaTeX code to the console
		System.out.println(latexCode);

		// 6) Optionally, save the LaTeX code to a file
		String outputPath = "graph_diagram.tex"; // Change as needed
		try (PrintWriter writer = new PrintWriter(new FileWriter(outputPath))) {
			writer.write(latexCode);
			System.out.println("LaTeX code written to: " + outputPath);
		} catch (IOException e) {
			System.err.println("Failed to write LaTeX code to file: " + e.getMessage());
			e.printStackTrace();
		}
	}
}