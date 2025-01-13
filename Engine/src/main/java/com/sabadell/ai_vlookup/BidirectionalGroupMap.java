package com.sabadell.ai_vlookup;

import org.yaml.snakeyaml.Yaml;
import java.io.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.io.IOException;
import java.io.File;
import java.net.URISyntaxException;

/**
 * This class loads reference and target group definitions (including headers
 * and their weights) from a YAML file. Then it builds:
 * 
 * 1. refToGroups: Map<headerName, List<GroupBlock>> 2. targetToGroups:
 * Map<headerName, List<GroupBlock>> 3. refGroupsToTargetGroups: Map<reference
 * GroupBlock, List<target GroupBlock>> 4. targetGroupsToRefGroups: Map<target
 * GroupBlock, List<reference GroupBlock>>
 */
public class BidirectionalGroupMap implements Serializable{
	private static final long serialVersionUID = 1L;

	// FIX: Now these maps are keyed by HeaderName -> List of GroupBlock(s)
	private Map<String, List<GroupBlock>> refToGroups = new HashMap<String, List<GroupBlock>>();
	private Map<String, List<GroupBlock>> tgtToGroups = new HashMap<String, List<GroupBlock>>();

	// Map for keeping track of the reference groups by name
	private Map<String, GroupBlock> refGroups = new HashMap<String, GroupBlock>();

	// Map for keeping track of the target groups by name
	private Map<String, GroupBlock> tgtGroups = new HashMap<String, GroupBlock>();

	// This maps from a single reference group-block to 0..N target group-block(s)
	private Map<String, List<GroupBlock>> refGroupsToTgtGroups = new HashMap<String, List<GroupBlock>>();
	// This maps from a single target group-block to 0..N reference group-block(s)
	private Map<String, List<GroupBlock>> tgtGroupsToRefGroups = new HashMap<String, List<GroupBlock>>();

	private transient Yaml base_yaml;

	/**
	 * Helper method for the constructor to retrieve YAML configuration file.
	 * 
	 * @param yamlFile The configuration.yaml file to load.
	 * @throws IOException
	 */
	private Map<String, Object> loadConfiguration(File yamlFile) throws IOException {
		// 1) Load YAML into a Map
		base_yaml = new Yaml();
		Map<String, Object> root;
		try (InputStream is = new FileInputStream(yamlFile)) {
			root = this.base_yaml.load(is);
		} catch (Exception e) {
			System.err.println("Error reading YAML file:\n" + e.toString());
			throw new IOException("YAML file is empty or invalid.");
		}

		return root;

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
	 * Constructor: loads the YAML file, parses everything, populates the data
	 * structures.
	 */
	public BidirectionalGroupMap(File yamlFile) throws IOException {

		Map<String, Object> root = this.loadConfiguration(yamlFile);

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
		Map<String, GroupBlock> refGroupMap = parseGroupBlocksFromYaml(referenceGroupsRaw, this.refGroups);

		// 4) Parse the target groups into a map: groupName -> GroupBlock
		Map<String, GroupBlock> targetGroupMap = parseGroupBlocksFromYaml(targetGroupsRaw, this.tgtGroups);

		// 5) Build refToGroups / targetToGroups, but keyed by header name
		for (Map.Entry<String, GroupBlock> entry : refGroupMap.entrySet()) {
			GroupBlock gb = entry.getValue();
			for (String header : gb.getHeaders()) {
				refToGroups.computeIfAbsent(header, k -> new ArrayList<>()).add(gb);
			}
		}

		for (Map.Entry<String, GroupBlock> entry : targetGroupMap.entrySet()) {
			GroupBlock gb = entry.getValue();
			for (String header : gb.getHeaders()) {
				tgtToGroups.computeIfAbsent(header, k -> new ArrayList<>()).add(gb);
			}
		}

		// 6) Parse the relationships into Map<String, List<String>>
		// (still groupName -> groupName(s))

		Map<String, List<String>> refToTgtRelationships = parseYamlRelationships(refToTgtRaw);
		Map<String, List<String>> tgtToRefRelationships = parseYamlRelationships(tgtToRefRaw);

		// 7) Build the final relationship maps:
		// refGroupsToTargetGroups: reference GroupBlock -> list of target GroupBlock(s)
		// targetGroupsToRefGroups: target GroupBlock -> list of reference GroupBlock(s)

		// -- For "Ref->Tgt": key is a target group name, value is a list of ref group
		// names
		// So for each T_GROUP name in the map, we get the T_GROUP block, then for each
		// ref group name that maps to that T_GROUP, we add that T_GROUP block to
		// refBlock's list.
		structureSingleDirectionGroupsToGroups(refToTgtRelationships, this.refGroupsToTgtGroups, tgtGroups);

		// -- For "Tgt->Ref": key is a reference group name, value is a list of target
		// group names
		// So for each REF group name, we map each T_GROUP block to that REF group block
		structureSingleDirectionGroupsToGroups(tgtToRefRelationships, this.tgtGroupsToRefGroups, refGroups);

	}

	/**
	 * Helper method to structure mappings between groups to groups.
	 * 
	 * @param rawMap     The map of the source groups parsed from the YAML file
	 * @param storeMap   The map to sore the parsed relationships
	 * @param endNameMap The current object's map that links end group names to
	 *                   their corresponding block.
	 */
	private void structureSingleDirectionGroupsToGroups(Map<String, List<String>> rawMap,
			Map<String, List<GroupBlock>> storeMap, Map<String, GroupBlock> endNameMap) {

		for (Map.Entry<String, List<String>> entry : rawMap.entrySet()) {

			String srcGroupName = entry.getKey();

			List<GroupBlock> endBlocks = new ArrayList<GroupBlock>();

			for (String endGroupName : entry.getValue()) {
				GroupBlock endGroupBlock = endNameMap.get(endGroupName);
				endBlocks.add(endGroupBlock);
			}

			storeMap.put(srcGroupName, endBlocks);

		}

	}

	/**
	 * Utility method to parse a YAML map of the form:
	 * 
	 * key: string OR list of strings
	 * 
	 * returning a normalized map:
	 * 
	 * key -> list of strings
	 * 
	 * For example, "InvoiceDetails": "Transactions" becomes "InvoiceDetails":
	 * ["Transactions"]
	 * 
	 * and "Address": ["Address", "Name"] stays the same.
	 */
	private Map<String, List<String>> parseYamlRelationships(Map<String, Object> rawMap) {
		Map<String, List<String>> result = new LinkedHashMap<>();
		if (rawMap == null) {
			return result; // or throw an exception if needed
		}

		for (Map.Entry<String, Object> entry : rawMap.entrySet()) {
			String key = entry.getKey();
			Object val = entry.getValue();

			// If "val" is a string => single-item list
			// If "val" is a list => cast to List<String>
			if (val instanceof String) {
				result.put(key, Collections.singletonList((String) val));
			} else if (val instanceof List) {
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
	 * Parse a YAML map like:
	 * 
	 * groupName -> list of "HeaderName(weight)" strings
	 * 
	 * returning a map:
	 * 
	 * groupName -> GroupBlock
	 */
	private Map<String, GroupBlock> parseGroupBlocksFromYaml(Map<String, List<String>> groupsMap,
			Map<String, GroupBlock> groupNameMap) throws IOException {
		Map<String, GroupBlock> groupMap = new HashMap<>();

		if (groupsMap == null) {
			return groupMap; // or throw an exception if needed
		}

		// We'll reuse the same regex logic as before
		Pattern p = Pattern.compile("(\\S+)\\((\\d+(\\.\\d+)?)\\)");

		for (Map.Entry<String, List<String>> entry : groupsMap.entrySet()) {
			String groupName = entry.getKey();
			List<String> rawHeaders = entry.getValue();
			if (rawHeaders == null || rawHeaders.isEmpty()) {
				continue;
			}

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
				groupNameMap.put(groupName, gb);

			}
		}
		return groupMap;
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
		// 1) Locate the YAML file
		java.net.URL yamlUrl = BidirectionalGroupMap.class.getResource("/header_configuration.yaml");
		if (yamlUrl == null) {
			throw new IllegalStateException("Could not find 'header_configuration.yaml' in resources folder!");
		}

		// 2) Convert that URL into a File object
		File yamlFile;
		try {
			yamlFile = new File(yamlUrl.toURI());
		} catch (URISyntaxException e) {
			// fallback if there's something off about the URI
			yamlFile = new File(yamlUrl.getPath());
		}

		// 3) Instantiate BidirectionalGroupMap using the YAML
		BidirectionalGroupMap map = new BidirectionalGroupMap(yamlFile);

		// 4) Generate the LaTeX graph code
		String latexCode = map.toLatexGraph();

		// 5) Print it out
		System.out.println(latexCode);

		// Optionally write it to a file, etc.
		// Files.write(Paths.get("my_diagram.tex"), latexCode.getBytes());
	}
}