//! Receiver filesystem operations anchored to real directory handles.

#[cfg(windows)]
use std::fs;
use std::fs::{File, Metadata};
use std::io;
use std::path::{Component, Path, PathBuf};

#[cfg(target_os = "linux")]
use std::os::fd::AsFd;
#[cfg(target_os = "linux")]
use std::os::unix::fs::MetadataExt;
#[cfg(windows)]
use std::os::windows::io::AsRawHandle;

#[cfg(windows)]
use std::os::windows::fs::{MetadataExt, OpenOptionsExt};

#[cfg(windows)]
use windows_sys::Win32::Foundation::{GENERIC_READ, GENERIC_WRITE};
#[cfg(windows)]
use windows_sys::Win32::Storage::FileSystem::{
    DELETE, FILE_APPEND_DATA, FILE_ATTRIBUTE_REPARSE_POINT, FILE_DISPOSITION_INFO,
    FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT, FILE_ID_INFO, FILE_RENAME_INFO,
    FILE_SHARE_READ, FILE_SHARE_WRITE, FileDispositionInfo, FileIdInfo, FileRenameInfo,
    GetFileInformationByHandleEx, SetFileInformationByHandle,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct FileIdentity {
    #[cfg(target_os = "linux")]
    device: u64,
    #[cfg(target_os = "linux")]
    inode: u64,
    #[cfg(windows)]
    volume: u64,
    #[cfg(windows)]
    file_id: [u8; 16],
}

pub struct PinnedFile {
    file: tokio::fs::File,
}

impl PinnedFile {
    pub fn file_mut(&mut self) -> &mut tokio::fs::File {
        &mut self.file
    }
}

#[cfg(windows)]
fn file_identity(file: &impl AsRawHandle) -> io::Result<FileIdentity> {
    let mut info = FILE_ID_INFO::default();
    // SAFETY: the handle and writable FILE_ID_INFO buffer remain valid for the call.
    let result = unsafe {
        GetFileInformationByHandleEx(
            file.as_raw_handle(),
            FileIdInfo,
            (&mut info as *mut FILE_ID_INFO).cast(),
            std::mem::size_of::<FILE_ID_INFO>() as u32,
        )
    };
    if result == 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(FileIdentity {
        volume: info.VolumeSerialNumber,
        file_id: info.FileId.Identifier,
    })
}

pub fn pin(file: tokio::fs::File) -> PinnedFile {
    PinnedFile { file }
}

fn source_identity(source: &PinnedFile) -> io::Result<FileIdentity> {
    #[cfg(target_os = "linux")]
    {
        let file = File::from(source.file.as_fd().try_clone_to_owned()?);
        let metadata = file.metadata()?;
        Ok(FileIdentity {
            device: metadata.dev(),
            inode: metadata.ino(),
        })
    }
    #[cfg(windows)]
    {
        file_identity(&source.file)
    }
}

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
        let mut flags = if read && (write || append) {
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
    promotable: bool,
) -> io::Result<File> {
    let (dir, name) = parent(path)?;
    #[cfg(target_os = "linux")]
    let file = linux::file(&dir, &name, read, write, append, create_new)?;
    #[cfg(windows)]
    let file = {
        let mut options = fs::OpenOptions::new();
        options
            .read(read)
            .write(write)
            .append(append)
            .create_new(create_new)
            .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE)
            .custom_flags(FILE_FLAG_OPEN_REPARSE_POINT);
        if promotable {
            let access = DELETE
                | if read { GENERIC_READ } else { 0 }
                | if append {
                    FILE_APPEND_DATA
                } else if write {
                    GENERIC_WRITE
                } else {
                    0
                };
            options.access_mode(access);
        }
        options.open(dir.path.join(name))?
    };
    #[cfg(target_os = "linux")]
    let _ = promotable;
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
    open_regular_std(path, read, write, append, create_new, false).map(tokio::fs::File::from_std)
}

pub fn open_promotable(
    path: &Path,
    read: bool,
    write: bool,
    append: bool,
    create_new: bool,
) -> io::Result<tokio::fs::File> {
    open_regular_std(path, read, write, append, create_new, true).map(tokio::fs::File::from_std)
}

pub fn metadata(path: &Path) -> io::Result<Metadata> {
    open_regular_std(path, true, false, false, false, false)?.metadata()
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

#[cfg(windows)]
fn rename_handle(source: &PinnedFile, destination: &Path) -> io::Result<()> {
    use std::os::windows::ffi::OsStrExt;

    let name = destination.as_os_str().encode_wide().collect::<Vec<_>>();
    let name_bytes = name
        .len()
        .checked_mul(2)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "destination too long"))?;
    let size = std::mem::offset_of!(FILE_RENAME_INFO, FileName)
        .checked_add(name_bytes)
        .and_then(|size| size.checked_add(2))
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "destination too long"))?;
    let size_u32 = u32::try_from(size)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "destination too long"))?;
    let mut buffer = vec![0u64; size.div_ceil(8)];
    let info = buffer.as_mut_ptr().cast::<FILE_RENAME_INFO>();
    // SAFETY: the aligned buffer has room for the header and UTF-16 filename;
    // source and buffer stay live until SetFileInformationByHandle returns.
    let result = unsafe {
        (*info).Anonymous.ReplaceIfExists = true;
        (*info).RootDirectory = std::ptr::null_mut();
        (*info).FileNameLength = name_bytes as u32;
        std::ptr::copy_nonoverlapping(name.as_ptr(), (*info).FileName.as_mut_ptr(), name.len());
        SetFileInformationByHandle(
            source.file.as_raw_handle(),
            FileRenameInfo,
            info.cast(),
            size_u32,
        )
    };
    if result == 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

fn promoted_identity(destination: &Path) -> io::Result<FileIdentity> {
    #[cfg(target_os = "linux")]
    {
        let (dir, name) = parent(destination)?;
        let file = linux::entry(&dir, &name)?;
        let metadata = file.metadata()?;
        reject_non_regular(&metadata)?;
        Ok(FileIdentity {
            device: metadata.dev(),
            inode: metadata.ino(),
        })
    }
    #[cfg(windows)]
    {
        let (dir, name) = parent(destination)?;
        let file = fs::OpenOptions::new()
            .access_mode(0)
            .share_mode(
                FILE_SHARE_READ
                    | FILE_SHARE_WRITE
                    | windows_sys::Win32::Storage::FileSystem::FILE_SHARE_DELETE,
            )
            .custom_flags(FILE_FLAG_OPEN_REPARSE_POINT)
            .open(dir.path.join(name))?;
        reject_non_regular(&file.metadata()?)?;
        file_identity(&file)
    }
}

fn validate_promotion(source: &PinnedFile, destination: &Path) -> io::Result<()> {
    if promoted_identity(destination)? != source_identity(source)? {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "published object differs from pinned source",
        ));
    }
    Ok(())
}

pub fn promote_rename(source: &PinnedFile, from: &Path, to: &Path) -> io::Result<()> {
    #[cfg(target_os = "linux")]
    {
        let (source_dir, source_name) = parent(from)?;
        let (target_dir, target_name) = parent(to)?;
        linux::rename(&source_dir, &source_name, &target_dir, &target_name)?;
    }
    #[cfg(windows)]
    {
        let _ = from;
        let (target_dir, target_name) = parent(to)?;
        rename_handle(source, &target_dir.path.join(target_name))?;
    }
    validate_promotion(source, to)
}

pub fn promote_link(source: &PinnedFile, from: &Path, to: &Path) -> io::Result<()> {
    let (source_dir, source_name) = parent(from)?;
    let (target_dir, target_name) = parent(to)?;
    #[cfg(target_os = "linux")]
    linux::hard_link(&source_dir, &source_name, &target_dir, &target_name)?;
    #[cfg(windows)]
    fs::hard_link(
        source_dir.path.join(source_name),
        target_dir.path.join(target_name),
    )?;
    validate_promotion(source, to)?;
    #[cfg(windows)]
    {
        let disposition = FILE_DISPOSITION_INFO { DeleteFile: true };
        // SAFETY: this handle still names the pinned source link and has DELETE access.
        // No other process can replace that name while it is open without FILE_SHARE_DELETE.
        if unsafe {
            SetFileInformationByHandle(
                source.file.as_raw_handle(),
                FileDispositionInfo,
                (&disposition as *const FILE_DISPOSITION_INFO).cast(),
                std::mem::size_of::<FILE_DISPOSITION_INFO>() as u32,
            )
        } == 0
        {
            return Err(io::Error::last_os_error());
        }
    }
    // On Linux, unlinking the source name could remove a different file placed
    // there after linkat. Keep the extra link instead.
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn regular_file_round_trip() {
        let temp = tempfile::tempdir().unwrap();
        ensure_directory(temp.path()).unwrap();
        let path = temp.path().join("test.tmp");
        let file = open_regular(&path, false, true, false, true).unwrap();
        drop(file);
        assert!(exists(&path).unwrap());
        metadata(&path).unwrap();
        let target = temp.path().join("test.final");
        let source = pin(open_promotable(&path, true, false, false, false).unwrap());
        promote_rename(&source, &path, &target).unwrap();
        assert_eq!(
            promoted_identity(&target).unwrap(),
            source_identity(&source).unwrap()
        );
        drop(source);
        remove_file(&target).unwrap();
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn renamed_regular_replacement_is_not_accepted() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("source");
        let displaced = temp.path().join("displaced");
        let target = temp.path().join("target");
        std::fs::write(&path, b"A").unwrap();
        let source = pin(open_promotable(&path, true, false, false, false).unwrap());
        std::fs::rename(&path, &displaced).unwrap();
        std::fs::write(&path, b"B").unwrap();

        let error = promote_rename(&source, &path, &target).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(std::fs::read(&target).unwrap(), b"B");
        assert_eq!(std::fs::read(&displaced).unwrap(), b"A");
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn linked_regular_replacement_is_not_accepted() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("source");
        let displaced = temp.path().join("displaced");
        let target = temp.path().join("target");
        std::fs::write(&path, b"A").unwrap();
        let source = pin(open_promotable(&path, true, false, false, false).unwrap());
        std::fs::rename(&path, &displaced).unwrap();
        std::fs::write(&path, b"B").unwrap();

        let error = promote_link(&source, &path, &target).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(std::fs::read(&target).unwrap(), b"B");
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn pinned_link_publishes_the_open_object() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("source");
        let target = temp.path().join("target");
        std::fs::write(&path, b"A").unwrap();
        let source = pin(open_promotable(&path, true, false, false, false).unwrap());
        promote_link(&source, &path, &target).unwrap();
        assert_eq!(std::fs::read(&target).unwrap(), b"A");
        assert_eq!(
            promoted_identity(&target).unwrap(),
            source_identity(&source).unwrap()
        );
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn renamed_symlink_replacement_is_not_accepted() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("source");
        let displaced = temp.path().join("displaced");
        let target = temp.path().join("target");
        std::fs::write(&path, b"A").unwrap();
        let source = pin(open_promotable(&path, true, false, false, false).unwrap());
        std::fs::rename(&path, &displaced).unwrap();
        std::os::unix::fs::symlink(&displaced, &path).unwrap();
        assert!(promote_rename(&source, &path, &target).is_err());
        assert_eq!(std::fs::read(&displaced).unwrap(), b"A");
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn linked_symlink_replacement_is_not_accepted() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("source");
        let displaced = temp.path().join("displaced");
        let target = temp.path().join("target");
        std::fs::write(&path, b"A").unwrap();
        let source = pin(open_promotable(&path, true, false, false, false).unwrap());
        std::fs::rename(&path, &displaced).unwrap();
        std::os::unix::fs::symlink(&displaced, &path).unwrap();
        assert!(promote_link(&source, &path, &target).is_err());
        assert_eq!(std::fs::read(&displaced).unwrap(), b"A");
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn pinned_rename_blocks_regular_replacement_and_moves_handle_object() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("source");
        let displaced = temp.path().join("displaced");
        let target = temp.path().join("target");
        std::fs::write(&path, b"A").unwrap();
        let source = pin(open_promotable(&path, true, false, false, false).unwrap());
        assert!(std::fs::rename(&path, &displaced).is_err());
        assert!(std::fs::remove_file(&path).is_err());
        promote_rename(&source, &path, &target).unwrap();
        assert_eq!(std::fs::read(&target).unwrap(), b"A");
        assert_eq!(
            promoted_identity(&target).unwrap(),
            source_identity(&source).unwrap()
        );
    }

    #[cfg(windows)]
    #[test]
    fn destination_identity_does_not_require_data_read_access() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("target");
        std::fs::write(&path, b"A").unwrap();
        let _writer = std::fs::OpenOptions::new()
            .write(true)
            .share_mode(
                windows_sys::Win32::Storage::FileSystem::FILE_SHARE_WRITE
                    | windows_sys::Win32::Storage::FileSystem::FILE_SHARE_DELETE,
            )
            .open(&path)
            .unwrap();
        assert!(promoted_identity(&path).is_ok());
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn pinned_link_blocks_replacement_and_removes_only_the_source_link() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("source");
        let displaced = temp.path().join("displaced");
        let target = temp.path().join("target");
        std::fs::write(&path, b"A").unwrap();
        let source = pin(open_promotable(&path, true, false, false, false).unwrap());
        assert!(std::fs::rename(&path, &displaced).is_err());
        promote_link(&source, &path, &target).unwrap();
        assert_eq!(
            promoted_identity(&target).unwrap(),
            source_identity(&source).unwrap()
        );
        drop(source);
        assert!(!path.exists());
        assert_eq!(std::fs::read(&target).unwrap(), b"A");
    }

    #[tokio::test]
    async fn pinned_link_preserves_existing_destination() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("source");
        let target = temp.path().join("target");
        std::fs::write(&path, b"A").unwrap();
        std::fs::write(&target, b"sentinel").unwrap();
        let source = pin(open_promotable(&path, true, false, false, false).unwrap());
        assert_eq!(
            promote_link(&source, &path, &target).unwrap_err().kind(),
            io::ErrorKind::AlreadyExists
        );
        assert_eq!(std::fs::read(&target).unwrap(), b"sentinel");
    }
}
