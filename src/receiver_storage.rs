//! Receiver filesystem operations anchored to real directory handles.

#[cfg(windows)]
use std::fs;
use std::fs::{File, Metadata};
use std::io;
use std::path::{Component, Path, PathBuf};

#[cfg(windows)]
use std::os::windows::fs::{MetadataExt, OpenOptionsExt};

#[cfg(windows)]
use windows_sys::Win32::Storage::FileSystem::{
    FILE_ATTRIBUTE_REPARSE_POINT, FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT,
    FILE_SHARE_READ, FILE_SHARE_WRITE,
};

#[cfg(windows)]
struct Directory {
    // Windows denies rename/delete of every ancestor while these handles live.
    _chain: Vec<File>,
    path: PathBuf,
}

#[cfg(target_os = "linux")]
struct Directory(File);

fn absolute(path: &Path) -> io::Result<PathBuf> {
    let path = std::path::absolute(path)?;
    if path
        .components()
        .any(|part| matches!(part, Component::ParentDir))
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "parent path component",
        ));
    }
    Ok(path)
}

#[cfg(target_os = "linux")]
fn directory_components(path: &Path) -> io::Result<(PathBuf, Vec<PathBuf>)> {
    let path = absolute(path)?;
    let mut ancestors = path.ancestors().map(Path::to_path_buf).collect::<Vec<_>>();
    ancestors.reverse();
    let root = ancestors.remove(0);
    Ok((root, ancestors))
}

#[cfg(windows)]
fn open_directory(path: &Path) -> io::Result<File> {
    let file = fs::OpenOptions::new()
        .read(true)
        .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT)
        .open(path)
        .map_err(|error| {
            io::Error::new(
                error.kind(),
                format!("opening directory {}: {error}", path.display()),
            )
        })?;
    let metadata = file.metadata()?;
    if !metadata.is_dir() || metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "directory is a reparse point or is not a directory",
        ));
    }
    Ok(file)
}

#[cfg(windows)]
fn directory(path: &Path, create: bool) -> io::Result<Directory> {
    let path = absolute(path)?;
    let internal = path.ancestors().find(|ancestor| {
        ancestor
            .file_name()
            .is_some_and(|name| name.to_string_lossy().eq_ignore_ascii_case(".warpfile"))
    });
    let anchor = internal
        .and_then(Path::parent)
        .and_then(Path::parent)
        .or_else(|| path.parent())
        .unwrap_or(&path);
    let mut descendants = path
        .ancestors()
        .take_while(|entry| *entry != anchor)
        .map(Path::to_path_buf)
        .collect::<Vec<_>>();
    descendants.reverse();
    let mut chain = vec![open_directory(anchor)?];
    for entry in descendants {
        if create {
            match fs::create_dir(&entry) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
                Err(error) => return Err(error),
            }
        }
        chain.push(open_directory(&entry)?);
    }
    Ok(Directory {
        _chain: chain,
        path,
    })
}

#[cfg(target_os = "linux")]
mod linux {
    use super::*;
    use std::ffi::CString;
    use std::os::fd::{AsRawFd, FromRawFd};
    use std::os::unix::ffi::OsStrExt;

    const O_WRONLY: i32 = 1;
    const O_RDWR: i32 = 2;
    const O_APPEND: i32 = 1024;
    const O_CREAT: i32 = 64;
    const O_EXCL: i32 = 128;
    const O_DIRECTORY: i32 = 65536;
    const O_NOFOLLOW: i32 = 131072;
    const O_CLOEXEC: i32 = 524288;
    const O_PATH: i32 = 2097152;

    unsafe extern "C" {
        fn openat(dirfd: i32, pathname: *const i8, flags: i32, mode: u32) -> i32;
        fn mkdirat(dirfd: i32, pathname: *const i8, mode: u32) -> i32;
        fn unlinkat(dirfd: i32, pathname: *const i8, flags: i32) -> i32;
        fn renameat(oldfd: i32, old: *const i8, newfd: i32, new: *const i8) -> i32;
        fn linkat(oldfd: i32, old: *const i8, newfd: i32, new: *const i8, flags: i32) -> i32;
    }

    fn name(path: &Path) -> io::Result<CString> {
        CString::new(path.as_os_str().as_bytes())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "NUL in path"))
    }

    fn opened(fd: i32) -> io::Result<File> {
        if fd < 0 {
            Err(io::Error::last_os_error())
        } else {
            // SAFETY: a successful openat returns a new owned file descriptor.
            Ok(unsafe { File::from_raw_fd(fd) })
        }
    }

    pub(super) fn open(dir: &Directory, path: &Path, flags: i32) -> io::Result<File> {
        let name = name(path)?;
        // SAFETY: the directory descriptor and NUL-terminated name stay live for this call.
        opened(unsafe {
            openat(
                dir.0.as_raw_fd(),
                name.as_ptr(),
                flags | O_CLOEXEC | O_NOFOLLOW,
                0o666,
            )
        })
    }

    pub(super) fn directory(path: &Path, create: bool) -> io::Result<Directory> {
        let (root, descendants) = directory_components(path)?;
        let mut dir = Directory(File::open(root)?);
        for entry in descendants {
            let component = entry
                .file_name()
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "invalid directory"))?;
            let component_name = Path::new(component);
            let component = name(component_name)?;
            if create {
                // SAFETY: the name and parent descriptor stay live for the call.
                let result = unsafe { mkdirat(dir.0.as_raw_fd(), component.as_ptr(), 0o777) };
                if result < 0 {
                    let error = io::Error::last_os_error();
                    if error.kind() != io::ErrorKind::AlreadyExists {
                        return Err(error);
                    }
                }
            }
            dir = Directory(open(&dir, component_name, O_DIRECTORY)?);
        }
        Ok(dir)
    }

    pub(super) fn file(
        dir: &Directory,
        path: &Path,
        read: bool,
        write: bool,
        append: bool,
        create_new: bool,
    ) -> io::Result<File> {
        let mut flags = if read && write {
            O_RDWR
        } else if write || append {
            O_WRONLY
        } else {
            0
        };
        if append {
            flags |= O_APPEND;
        }
        if create_new {
            flags |= O_CREAT | O_EXCL;
        }
        open(dir, path, flags)
    }

    pub(super) fn entry(dir: &Directory, path: &Path) -> io::Result<File> {
        open(dir, path, O_PATH)
    }

    pub(super) fn remove(dir: &Directory, path: &Path) -> io::Result<()> {
        let name = name(path)?;
        // SAFETY: pointers and parent descriptor stay live for the call.
        if unsafe { unlinkat(dir.0.as_raw_fd(), name.as_ptr(), 0) } == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }

    pub(super) fn rename(
        from: &Directory,
        source: &Path,
        to: &Directory,
        target: &Path,
    ) -> io::Result<()> {
        let source = name(source)?;
        let target = name(target)?;
        // SAFETY: pointers and both directory descriptors stay live for the call.
        if unsafe {
            renameat(
                from.0.as_raw_fd(),
                source.as_ptr(),
                to.0.as_raw_fd(),
                target.as_ptr(),
            )
        } == 0
        {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }

    pub(super) fn hard_link(
        from: &Directory,
        source: &Path,
        to: &Directory,
        target: &Path,
    ) -> io::Result<()> {
        let source = name(source)?;
        let target = name(target)?;
        // SAFETY: pointers and both directory descriptors stay live for the call.
        if unsafe {
            linkat(
                from.0.as_raw_fd(),
                source.as_ptr(),
                to.0.as_raw_fd(),
                target.as_ptr(),
                0,
            )
        } == 0
        {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }
}

#[cfg(target_os = "linux")]
use linux::directory;

fn parent(path: &Path) -> io::Result<(Directory, PathBuf)> {
    let path = absolute(path)?;
    let parent = path
        .parent()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "path has no parent"))?;
    let name = path
        .file_name()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "path has no filename"))?;
    Ok((directory(parent, false)?, PathBuf::from(name)))
}

pub fn ensure_directory(path: &Path) -> io::Result<()> {
    let _ = directory(path, true)?;
    Ok(())
}

pub fn exists(path: &Path) -> io::Result<bool> {
    let (dir, name) = match parent(path) {
        Ok(parent) => parent,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error),
    };
    #[cfg(target_os = "linux")]
    let result = linux::entry(&dir, &name).map(|_| ());
    #[cfg(windows)]
    let result = fs::symlink_metadata(dir.path.join(name)).map(|_| ());
    match result {
        Ok(()) => Ok(true),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error),
    }
}

fn reject_non_regular(metadata: &Metadata) -> io::Result<()> {
    #[cfg(windows)]
    if metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "file is a reparse point",
        ));
    }
    if !metadata.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "not a regular file",
        ));
    }
    Ok(())
}

fn open_regular_std(
    path: &Path,
    read: bool,
    write: bool,
    append: bool,
    create_new: bool,
) -> io::Result<File> {
    let (dir, name) = parent(path)?;
    #[cfg(target_os = "linux")]
    let file = linux::file(&dir, &name, read, write, append, create_new)?;
    #[cfg(windows)]
    let file = fs::OpenOptions::new()
        .read(read)
        .write(write)
        .append(append)
        .create_new(create_new)
        .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE)
        .custom_flags(FILE_FLAG_OPEN_REPARSE_POINT)
        .open(dir.path.join(name))?;
    reject_non_regular(&file.metadata()?)?;
    Ok(file)
}

pub fn open_regular(
    path: &Path,
    read: bool,
    write: bool,
    append: bool,
    create_new: bool,
) -> io::Result<tokio::fs::File> {
    open_regular_std(path, read, write, append, create_new).map(tokio::fs::File::from_std)
}

pub fn metadata(path: &Path) -> io::Result<Metadata> {
    open_regular_std(path, true, false, false, false)?.metadata()
}

pub async fn read(path: &Path) -> io::Result<Vec<u8>> {
    use tokio::io::AsyncReadExt;
    let mut file = open_regular(path, true, false, false, false)?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes).await?;
    Ok(bytes)
}

pub fn remove_file(path: &Path) -> io::Result<()> {
    let (dir, name) = parent(path)?;
    let metadata = match metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    };
    reject_non_regular(&metadata)?;
    #[cfg(target_os = "linux")]
    return linux::remove(&dir, &name);
    #[cfg(windows)]
    return fs::remove_file(dir.path.join(name));
}

pub fn rename(from: &Path, to: &Path) -> io::Result<()> {
    metadata(from)?;
    let (source_dir, source_name) = parent(from)?;
    let (target_dir, target_name) = parent(to)?;
    #[cfg(target_os = "linux")]
    linux::rename(&source_dir, &source_name, &target_dir, &target_name)?;
    #[cfg(windows)]
    fs::rename(
        source_dir.path.join(source_name),
        target_dir.path.join(target_name),
    )?;
    metadata(to).map(|_| ())
}

pub fn hard_link(from: &Path, to: &Path) -> io::Result<()> {
    metadata(from)?;
    let (source_dir, source_name) = parent(from)?;
    let (target_dir, target_name) = parent(to)?;
    #[cfg(target_os = "linux")]
    linux::hard_link(&source_dir, &source_name, &target_dir, &target_name)?;
    #[cfg(windows)]
    fs::hard_link(
        source_dir.path.join(source_name),
        target_dir.path.join(target_name),
    )?;
    metadata(to).map(|_| ())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn regular_file_round_trip() {
        let temp = tempfile::tempdir().unwrap();
        ensure_directory(temp.path()).unwrap();
        let path = temp.path().join("test.tmp");
        let file = open_regular(&path, false, true, false, true).unwrap();
        drop(file);
        assert!(exists(&path).unwrap());
        metadata(&path).unwrap();
        let target = temp.path().join("test.final");
        rename(&path, &target).unwrap();
        remove_file(&target).unwrap();
    }
}
