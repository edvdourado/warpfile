use std::io::{self, Write};
use std::time::{Duration, Instant};

const DRAW_INTERVAL: Duration = Duration::from_millis(250);

pub struct ProgressTracker {
    label: &'static str,
    total_bytes: u64,
    transferred_bytes: u64,
    started_at: Instant,
    last_draw_at: Instant,
    last_line_length: usize,
}

impl ProgressTracker {
    pub fn new(label: &'static str, total_bytes: u64) -> Self {
        let now = Instant::now();

        Self {
            label,
            total_bytes,
            transferred_bytes: 0,
            started_at: now,
            last_draw_at: now,
            last_line_length: 0,
        }
    }

    pub fn add(&mut self, bytes: usize) {
        self.transferred_bytes = self.transferred_bytes.saturating_add(bytes as u64);

        if self.last_draw_at.elapsed() >= DRAW_INTERVAL {
            self.draw();

            self.last_draw_at = Instant::now();
        }
    }

    pub fn finish(&mut self) {
        self.transferred_bytes = self.total_bytes;

        self.draw();

        println!();
    }

    fn draw(&mut self) {
        let elapsed = self.started_at.elapsed().as_secs_f64();

        let percentage = if self.total_bytes == 0 {
            100.0
        } else {
            (self.transferred_bytes as f64 / self.total_bytes as f64) * 100.0
        };

        let speed = if elapsed > 0.0 {
            self.transferred_bytes as f64 / elapsed
        } else {
            0.0
        };

        let eta = if self.transferred_bytes >= self.total_bytes {
            "0s".to_string()
        } else if speed > 0.0 {
            let remaining = self.total_bytes - self.transferred_bytes;

            format_eta(remaining as f64 / speed)
        } else {
            "--".to_string()
        };

        let line = format!(
            "{}: {} / {} | {:>5.1}% | {}/s | ETA {}",
            self.label,
            format_bytes(self.transferred_bytes),
            format_bytes(self.total_bytes),
            percentage,
            format_bytes_f64(speed),
            eta,
        );

        let width = self.last_line_length.max(line.len());

        print!("\r{line:<width$}", width = width,);

        let _ = io::stdout().flush();

        self.last_line_length = line.len();
    }
}

fn format_bytes(bytes: u64) -> String {
    format_bytes_f64(bytes as f64)
}

fn format_bytes_f64(bytes: f64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];

    let mut value = bytes;
    let mut unit_index = 0;

    while value >= 1024.0 && unit_index < UNITS.len() - 1 {
        value /= 1024.0;
        unit_index += 1;
    }

    if unit_index == 0 {
        format!("{value:.0} {}", UNITS[unit_index])
    } else {
        format!("{value:.2} {}", UNITS[unit_index])
    }
}

fn format_eta(seconds: f64) -> String {
    if !seconds.is_finite() || seconds < 0.0 {
        return "--".to_string();
    }

    let total_seconds = seconds.ceil() as u64;

    if total_seconds < 60 {
        return format!("{total_seconds}s");
    }

    if total_seconds < 3600 {
        let minutes = total_seconds / 60;

        let seconds = total_seconds % 60;

        return format!("{minutes}m {seconds}s");
    }

    let hours = total_seconds / 3600;

    let minutes = (total_seconds % 3600) / 60;

    let seconds = total_seconds % 60;

    format!("{hours}h {minutes}m {seconds}s")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_bytes() {
        assert_eq!(format_bytes(0), "0 B");

        assert_eq!(format_bytes(1024), "1.00 KiB");

        assert_eq!(format_bytes(1024 * 1024), "1.00 MiB");
    }

    #[test]
    fn formats_short_eta() {
        assert_eq!(format_eta(12.2), "13s");
    }

    #[test]
    fn formats_long_eta() {
        assert_eq!(format_eta(3661.0), "1h 1m 1s");
    }
}
