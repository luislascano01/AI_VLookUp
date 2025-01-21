package com.sabadell.ai_vlookup;

import java.io.IOException;
import java.util.Collections;

import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import org.jline.utils.AttributedStringBuilder;
import org.jline.utils.AttributedStyle;
import org.jline.utils.InfoCmp;

public class ProgressDisplay {
	private Terminal terminal;

	private int prev_Progress = 0;
	private boolean total_Exceeds = false;
	private int count_Steps = 0;
	private static final int max_Bar_Size = 60;
	private int bar_Size;
	private int autoIterCount = 0;
	private int total;
	private boolean displayOpen = true;

	public ProgressDisplay(int total) {
		this.total = total;

		Terminal terminal = null;
		try {
			terminal = TerminalBuilder.terminal();
		} catch (Exception e) {
			System.err.println("Error when trying to create terminal for progress bar.");
		}
		this.count_Steps = total;
		this.terminal = terminal;
		if (total >= max_Bar_Size) {
			this.total_Exceeds = true;
			this.bar_Size = max_Bar_Size;
		} else {
			this.bar_Size = total;
		}
	}

	public void displayProgressAuto() throws IOException {
		if (this.autoIterCount + 1 >= total) {
			this.finishProgress();
		}
		this.displayProgress(autoIterCount);

		autoIterCount++;
	}

	public void displayProgress(int progress) throws IOException {
		if (displayOpen) {
			int scaled_Progress = progress;
			if (total_Exceeds) {
				scaled_Progress = (int) Math.ceil((progress / Double.parseDouble(this.count_Steps + "") * bar_Size));
			}

			if (prev_Progress <= progress) {
				terminal.puts(InfoCmp.Capability.carriage_return); // Move cursor to the beginning of the line
				String progressBar = new AttributedStringBuilder()
						.append("[", AttributedStyle.DEFAULT.foreground(AttributedStyle.YELLOW))
						.append(String.join("", Collections.nCopies(scaled_Progress, "=")),
								AttributedStyle.DEFAULT.foreground(AttributedStyle.GREEN))
						.append(">").append(String.join("", Collections.nCopies(bar_Size - scaled_Progress, " ")))
						.append("]", AttributedStyle.DEFAULT.foreground(AttributedStyle.YELLOW))
						.append(String.format(" %d%%", (100 * scaled_Progress / bar_Size))).toAnsi(terminal);
				terminal.writer().write(progressBar);
				terminal.writer().flush();
			} else {
				throw new IOException("Progress bar cannot be decreased.");
			}
			prev_Progress = progress;
		}
	}

	public void finishProgress() throws IOException {
		this.displayProgress(total);
		this.displayOpen = false;
		System.out.println("");
	}

}