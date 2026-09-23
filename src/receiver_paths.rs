use std::path::{Component, Path, PathBuf};

const INTERNAL_DIRECTORY: &str = ".warpfile";

pub fn internal_directory(destination_directory: &Path) -> PathBuf {
    destination_directory.join(INTERNAL_DIRECTORY)
}

pub fn partials_directory(destination_directory: &Path) -> PathBuf {
    internal_directory(destination_directory).join("partials")
}

pub fn partial_path(destination_directory: &Path, filename: &str) -> PathBuf {
    partials_directory(destination_directory).join(format!("{filename}.part"))
}

pub fn final_path(destination_directory: &Path, filename: &str) -> PathBuf {
    destination_directory.join(filename)
}

pub fn is_safe_filename(filename: &str) -> bool {
    if filename.is_empty()
        || filename.contains('/')
        || filename.contains('\\')
        || conflicts_with_internal_directory(filename)
    {
        return false;
    }

    let mut components = Path::new(filename).components();

    matches!(
        (components.next(), components.next()),
        (Some(Component::Normal(_)), None)
    )
}

fn conflicts_with_internal_directory(filename: &str) -> bool {
    /*
     * Windows treats trailing dots/spaces specially in ordinary Win32
     * path handling, so names such as ".warpfile." and ".warpfile "
     * must not be allowed to alias the internal directory.
     *
     * A colon may also address an alternate data stream on Windows.
     * Only the base component is relevant when deciding whether the
     * filename aliases WarpFile's reserved namespace.
     *
     * Apply this rule on every platform so filename acceptance remains
     * predictable across Windows and Unix.
     */
    let base = filename
        .split_once(':')
        .map_or(filename, |(base, _stream)| base);

    let normalized_base = base.trim_end_matches([' ', '.']);

    normalized_base.eq_ignore_ascii_case(INTERNAL_DIRECTORY)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reserves_internal_directory_on_all_platforms() {
        for filename in [
            ".warpfile",
            ".WARPFILE",
            ".warpfile.",
            ".warpfile ",
            ".WARPFILE...   ",
            ".warpfile:stream",
            ".WARPFILE::$DATA",
        ] {
            assert!(
                !is_safe_filename(filename),
                "{filename:?} must not alias the internal WarpFile directory"
            );
        }

        assert!(is_safe_filename("x.part.warpmeta"));
        assert!(is_safe_filename("x.part.warpchunks"));
    }

    #[test]
    fn partial_and_final_paths_have_distinct_parents() {
        let destination = Path::new("received");

        assert_eq!(
            partial_path(destination, "x"),
            destination.join(".warpfile/partials/x.part")
        );

        assert_eq!(
            final_path(destination, "x.part"),
            destination.join("x.part")
        );
    }
}
