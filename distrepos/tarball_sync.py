
import logging
from pathlib import Path
from distrepos.error import DiskFullError, TagFailure
from distrepos.params import Options, Tag
from distrepos.tag_run import update_release_repos
from distrepos.util import log_rsync, rsync_disk_is_full, rsync_with_link
from typing import Tuple, List

_log = logging.getLogger(__name__)

def tarball_sync(options: Options) -> Tuple[bool, str]:
    """
    rsync the tarball clients from vdt to local storage
    """
    _log.debug("tarball_sync")

    tarball_rsync = options.tarball_rsync
    working_dir = Path(options.working_root) / options.tarball_install
    dest_dir = Path(options.dest_root) / options.tarball_install

    description = f"rsync from tarball repo"
    ok, proc = rsync_with_link(tarball_rsync, working_dir, dest_dir, links=True)
    log_rsync(proc, description)
    if ok:
        _log.info("%s ok", description)
    else:
        if rsync_disk_is_full(proc):
            raise DiskFullError(description)
        return False, f"Error pulling tarball clients: {proc.stderr}"
    
    return True, ""

class TarballInfo():
    full_path: Path
    date_string: str
    os: str
    arch: str

    def __init__(self, tarball_path: Path):
        self.full_path = tarball_path
        name_parts = tarball_path.name.split('.')
        if len(name_parts) >= 5:
            self.arch = name_parts[-3]
            self.os = name_parts[-4]
            self.date_string = name_parts[-5]

    def is_valid(self):
        """ Check whether values for all fields were parsed from the file name """
        return self.date_string and self.os and self.arch

WN_TARBALL_NAME_PREFIX = "osg-wn-client-latest"

def create_latest_symlinks(options: Options) -> Tuple[bool, str]:
    """
    For each tarball client directory synced via rsync, create a "latest"
    symlink for each synced arch and el version
    """
    # TODO this assumes a number of things about the structure of downloaded tarball directories
    # We probably want to parameterize this somewhere
    # Assuming tarballs are rsynced into <dest_root>/<tarball_install>/<series>/<arch>/<name>.<os>.<arch>.tar.gz
    # Create a "latest" symlink in each <dest_root>/<tarball_install>/<series> for each <os>, <arch> combination

    working_dir = Path(options.working_root) / options.tarball_install

    for series_dir in working_dir.iterdir():
        if not series_dir.is_dir():
            continue
        
        for arch_dir in series_dir.iterdir():
            if not arch_dir.is_dir():
                continue

            infos = [TarballInfo(f) for f in arch_dir.iterdir() if f.is_file()]

            valid_infos = [i for i in infos if i.is_valid()]

            if len(valid_infos) != len(infos):
                # Treat unparsable file names as a warning rather than an error
                _log.warning(f"Found {len(infos) - len(valid_infos)} unparsable tarball file names in {arch_dir}")

            # Sanity check that each arch subdir only contains tarballs for a single arch
            arches = set(i.arch for i in valid_infos)
            if len(arches) != 1:
                return False, f"Got mixed set of arches for tarball clients in {arch_dir}"

            arch = next(a for a in arches)

            # Find the most recent tarball for each os version, sorted by OS
            oses = set(i.os for i in infos)
            for os in oses:
                latest_symlink = series_dir / f"{WN_TARBALL_NAME_PREFIX}.{os}.{arch}.tar.gz"
                os_tarballs = [i for i in infos if i.os == os]
                os_tarballs.sort(key=lambda i: i.date_string, reverse=True)
                latest_os_tarball = os_tarballs[0].full_path

                latest_symlink.symlink_to(latest_os_tarball.relative_to(series_dir))
    
    return True, ""



def update_tarball_dirs(options: Options) -> Tuple[bool, str]:
    """
    Rsync tarball client files from the upstream to local storage, then symlink the "latest"
    tarball for each directory.
    """

    working_dir = Path(options.working_root) / options.tarball_install
    dest_dir = Path(options.dest_root) / options.tarball_install
    prev_dir = Path(options.previous_root) / options.tarball_install

    working_dir.mkdir(parents=True, exist_ok=True)
    dest_dir.mkdir(parents=True, exist_ok=True)
    prev_dir.mkdir(parents=True, exist_ok=True)

    # Sync tarballs
    ok, err = tarball_sync(options)
    if not ok:
        return False, err

    # Create symlinkes
    ok, err = create_latest_symlinks(options)
    if not ok:
        return False, err
    
    # Move working dir to dest dir
    update_release_repos(dest_dir, working_dir, prev_dir)
    return True, ""

