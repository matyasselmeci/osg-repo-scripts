import logging
import os
import shutil
import string
import subprocess as sp
import tempfile
import typing as t
from pathlib import Path

from distrepos.__main__ import _debug  # TODO
from distrepos.error import TagFailure
from distrepos.params import Options, Tag
from distrepos.util import (
    RSYNC_NOT_FOUND,
    RSYNC_OK,
    acquire_lock,
    log_rsync,
    release_lock,
    rsync,
    rsync_with_link,
    run_with_log,
)

_log = logging.getLogger(__name__)


#
# The overall task runners
#


def get_koji_latest_dir(koji_rsync: str, tagdir: str) -> str:
    """
    Resolves the "latest" symlink for the dist-repo on koji-hub by downloading
    the symlink to a temporary directory and reading it.  (We don't want to use
    the "latest" symlink directly since it may change mid-run.)
    """
    with tempfile.TemporaryDirectory() as tempdir:
        destpath = os.path.join(tempdir, "latest")
        try:
            ok, proc = rsync(
                "-l", f"{koji_rsync}/{tagdir}/latest", destpath, timeout=180
            )
        except sp.TimeoutExpired:
            raise TagFailure("Timeout getting 'latest' dir")
        log_rsync(proc, "Getting 'latest' dir symlink")
        if not ok:
            if proc.returncode == RSYNC_NOT_FOUND:
                raise TagFailure(
                    "'latest' dir not found; dist-repo may not have been "
                    "run for this tag"
                )
            else:
                raise TagFailure("Error getting 'latest' dir")
        # we have copied the "latest" symlink as a (now broken) symlink. Read the text of the link to get
        # the directory on the remote side.
        return os.path.basename(os.readlink(destpath))


def rsync_from_koji(source_url, dest_path, link_path):
    """
    rsync the distrepo from kojihub for one tag, linking to the RPMs in
    the previous repo if they exist
    """
    _log.debug("rsync_from_koji(%r, %r, %r)", source_url, dest_path, link_path)
    description = f"rsync from {source_url} to {dest_path}"
    ok, proc = rsync_with_link(source_url, dest_path, link_path)
    log_rsync(proc, description)
    if not ok:
        raise TagFailure(f"Error with {description}")
    _log.info("%s ok", description)


def pull_condor_repos(options: Options, tag: Tag):
    """
    rsync binary and source RPMs from condor repos defined for this tag.
    """
    _log.debug("pull_condor_repos(%r)", tag.name)

    condor_rsync = options.condor_rsync
    working_root = options.working_root
    dest_root = options.dest_root

    # Condor SRPMS are in a subdirectory of the arch-specific condor-directory.
    # We do not do a recursive rsync because we prefer to put the SRPMS elsewhere.

    def sub_arch(a_string):
        """
        Substitute the current `arch` for `$ARCH` in a string
        """
        return string.Template(a_string).safe_substitute({"ARCH": arch})

    for idx, arch in enumerate(tag.arches):
        for repo in tag.condor_repos:
            arch_rpms_src = sub_arch(f"{condor_rsync}/{repo.src}/")
            debug_rpms_src = arch_rpms_src + "debug/"
            source_rpms_src = arch_rpms_src + "SRPMS/"

            arch_rpms_dst = sub_arch(f"{working_root}/{tag.arch_rpms_dest}/{repo.dst}/")
            debug_rpms_dst = sub_arch(
                f"{working_root}/{tag.debug_rpms_dest}/{repo.dst}/"
            )
            source_rpms_dst = sub_arch(
                f"{working_root}/{tag.source_rpms_dest}/{repo.dst}/"
            )

            arch_rpms_link = sub_arch(f"{dest_root}/{tag.arch_rpms_dest}/{repo.dst}/")
            debug_rpms_link = sub_arch(f"{dest_root}/{tag.debug_rpms_dest}/{repo.dst}/")
            source_rpms_link = sub_arch(
                f"{dest_root}/{tag.source_rpms_dest}/{repo.dst}/"
            )

            # First, pull the main (binary) RPMs
            description = f"rsync from condor repo for {arch} RPMs"
            ok, proc = rsync_with_link(
                arch_rpms_src + "*.rpm",
                arch_rpms_dst,
                arch_rpms_link,
                delete=False,
                recursive=False,
            )
            log_rsync(proc, description)
            if ok:
                _log.info("%s ok", description)
            else:
                raise TagFailure(f"Error pulling condor repos: {description}")

            # Next pull the debuginfo RPMs.  These may not exist.
            description = f"rsync from condor repo for {arch} debug RPMs"
            _, proc = rsync_with_link(
                debug_rpms_src + "*.rpm",
                debug_rpms_dst,
                debug_rpms_link,
                delete=False,
                recursive=False,
            )
            log_rsync(proc, description, not_found_is_ok=True)
            if proc.returncode not in {RSYNC_OK, RSYNC_NOT_FOUND}:
                raise TagFailure(f"Error pulling condor repos: {description}")
            else:
                _log.info("%s ok", description)

            # Finally pull the SRPMs -- these are identical between arches so only
            # pull if we're on the first arch.
            if idx == 0:
                description = f"rsync from condor repo for source RPMs"
                ok, proc = rsync_with_link(
                    source_rpms_src + "*.rpm",
                    source_rpms_dst,
                    source_rpms_link,
                    delete=False,
                    recursive=False,
                )
                log_rsync(proc, description)
                if ok:
                    _log.info("%s ok", description)
                else:
                    raise TagFailure(f"Error pulling condor repos: {description}")


def update_pkglist_files(working_path: Path, arches: t.List[str]):
    """
    Update the "pkglist" files with the relative paths of the RPM files, including
    files that were pulled from the condor repos.  Put debuginfo files in a separate
    pkglist.
    """
    _log.debug("update_pkglist_files(%r, %r)", working_path, arches)
    # Update pkglist files for SRPMs.  There's no such thing as a debuginfo SRPM so
    # we don't have to handle those.
    src_dir = working_path / "src"
    src_pkglist = src_dir / "pkglist"
    src_packages_dir = src_dir / "Packages"

    if not src_dir.exists():
        raise TagFailure(
            f"No {src_dir} directory found; the repo may not have been "
            "generated with the right options (--with-src)"
        )
    try:
        with open(f"{src_pkglist}.new", "wt") as new_pkglist_fh:
            # Walk the Packages directory tree and add the relative paths to the RPMs
            # (relative from src_dir) to the pkglist file.
            # Using os.walk() because Path.walk() is not available in Python 3.6
            for dirpath, _, filenames in os.walk(src_packages_dir):
                for fn in filenames:
                    if not fn.endswith(".src.rpm"):
                        continue
                    rpm_path = os.path.join(os.path.relpath(dirpath, src_dir), fn)
                    print(rpm_path, file=new_pkglist_fh)

        # New file written; move it into place, overwriting the old one.
        shutil.move(f"{src_pkglist}.new", src_pkglist)
        _log.info("Updating %s ok", src_pkglist)
    except OSError as err:
        raise TagFailure(f"OSError updating pkglist file {src_pkglist}: {err}") from err

    # Update pkglist files for binary RPMs for each arch.  Each arch has its
    # own directory with a pkglist file, and a debug subdirectory with another
    # pkglist file.  However, the binary RPMs themselves are mixed together.
    for arch in arches:
        arch_dir = working_path / arch
        arch_pkglist = arch_dir / "pkglist"
        arch_packages_dir = arch_dir / "Packages"
        arch_debug_dir = arch_dir / "debug"
        arch_debug_pkglist = arch_debug_dir / "pkglist"
        try:
            arch_debug_dir.mkdir(parents=True, exist_ok=True)
            # We have one directory tree to walk but two files to write.
            with open(f"{arch_pkglist}.new", "wt") as new_pkglist_fh, open(
                f"{arch_debug_pkglist}.new", "wt"
            ) as new_debug_pkglist_fh:

                # Walk the Packages directory tree and add the relative paths to the RPMs
                # (relative from src_dir) to the appropriate pkglist file.
                # Using os.walk() because Path.walk() is not available in Python 3.6
                for dirpath, _, filenames in os.walk(arch_packages_dir):
                    for fn in filenames:
                        if not fn.endswith(".rpm"):
                            continue
                        if "-debuginfo" in fn or "-debugsource" in fn:
                            # debuginfo/debugsource RPMs go into the debug pkglist and are relative to the debug dir
                            # which means including a '..'
                            rpm_path = os.path.join(
                                os.path.relpath(dirpath, arch_debug_dir), fn
                            )
                            print(rpm_path, file=new_debug_pkglist_fh)
                        else:
                            rpm_path = os.path.join(
                                os.path.relpath(dirpath, arch_dir), fn
                            )
                            print(rpm_path, file=new_pkglist_fh)

            # New files written; move them into place, overwriting old ones.
            shutil.move(f"{arch_pkglist}.new", arch_pkglist)
            _log.info("Updating %s ok", arch_pkglist)
            shutil.move(f"{arch_debug_pkglist}.new", arch_debug_pkglist)
            _log.info("Updating %s ok", arch_debug_pkglist)
        except OSError as err:
            raise TagFailure(
                f"OSError updating pkglist files {arch_pkglist} and {arch_debug_pkglist}: {err}"
            ) from err


def run_createrepo(working_path: Path, arches: t.List[str]):
    """
    Run createrepo on the main, source, and debuginfo dirs under the given
    working path.
    """
    _log.debug("run_createrepo(%r, %r)", working_path, arches)

    # SRPMS
    src_dir = working_path / "src"
    src_pkglist = src_dir / "pkglist"

    ok, proc = run_with_log(["createrepo_c", str(src_dir), f"--pkglist={src_pkglist}"])
    description = "running createrepo on SRPMs"
    if ok:
        _log.info("%s ok", description)
    else:
        raise TagFailure(f"Error {description}")

    # arch-specific packages and debug repos
    for arch in arches:
        arch_dir = working_path / arch
        arch_pkglist = arch_dir / "pkglist"
        ok, proc = run_with_log(
            ["createrepo_c", str(arch_dir), f"--pkglist={arch_pkglist}"]
        )
        description = f"running createrepo on {arch} rpms"
        if ok:
            _log.info("%s ok", description)
        else:
            raise TagFailure(f"Error {description}")

        arch_debug_dir = arch_dir / "debug"
        arch_debug_pkglist = arch_debug_dir / "pkglist"
        if not arch_debug_dir.exists():
            continue
        ok, proc = run_with_log(
            ["createrepo_c", str(arch_debug_dir), f"--pkglist={arch_debug_pkglist}"]
        )
        description = f"running createrepo on {arch} debuginfo rpms"
        if ok:
            _log.info("%s ok", description)
        else:
            raise TagFailure(f"Error {description}")


def create_compat_symlink(working_path: Path):
    """
    Create a symlink from
        <repo>/source/SRPMS (mash layout) -> <repo>/src (distrepo layout)
    (this needs to be a relative symlink because we're moving directories around)
    """
    _log.debug("_create_compat_symlink(%r)", working_path)
    description = "creating SRPM compat symlink"
    try:
        (working_path / "source").mkdir(parents=True, exist_ok=True)
        if (working_path / "source/SRPMS").exists():
            shutil.rmtree(working_path / "source/SRPMS")
        os.symlink("../src", working_path / "source/SRPMS")
    except OSError as err:
        raise TagFailure(f"Error {description}") from err
    _log.info("%s ok", description)


def update_release_repos(release_path: Path, working_path: Path, previous_path: Path):
    """
    Update the published repos by moving the published dir to the 'previous' dir
    and the working dir to the published dir.
    """
    _log.debug(
        "update_release_repos(%r, %r, %r)",
        release_path,
        working_path,
        previous_path,
    )
    failmsg = "Error updating release repos at %s" % release_path
    # Sanity check: make sure we have something to move
    if not working_path.exists():
        _log.error("Cannot release new dir %s: it does not exist", working_path)
        raise TagFailure(failmsg)

    # If we have an old previous path, clear it; also make sure its parents exist.
    if previous_path.exists():
        try:
            shutil.rmtree(previous_path)
        except OSError as err:
            _log.error(
                "OSError clearing previous dir %s: %s",
                previous_path,
                err,
                exc_info=_debug,
            )
            raise TagFailure(failmsg)
    previous_path.parent.mkdir(parents=True, exist_ok=True)

    # If we already have something in the release path, move it to the previous path.
    # Also create the parent dirs if necessary.
    if release_path.exists():
        try:
            shutil.move(release_path, previous_path)
        except OSError as err:
            _log.error(
                "OSError moving release dir %s to previous dir %s: %s",
                release_path,
                previous_path,
                err,
                exc_info=_debug,
            )
            raise TagFailure(failmsg)
    release_path.parent.mkdir(parents=True, exist_ok=True)

    # Now move the newly created repo to the release path.
    try:
        shutil.move(working_path, release_path)
    except OSError as err:
        _log.error(
            "OSError moving working dir %s to release dir %s: %s",
            working_path,
            release_path,
            err,
            exc_info=_debug,
        )
        # Something failed. Undo, undo!
        if previous_path.exists():
            try:
                shutil.move(previous_path, release_path)
            except OSError as err2:
                _log.error(
                    "OSError moving previous dir %s back to release dir %s: %s",
                    previous_path,
                    release_path,
                    err2,
                    exc_info=_debug,
                )
        raise TagFailure(failmsg)
    _log.info("Successfully released %s", release_path)


def run_one_tag(options: Options, tag: Tag) -> t.Tuple[bool, str]:
    """
    Run all the actions necessary to create a repo for one tag in the config.

    Args:
        options: The global options for the run
        tag: The specific tag to run actions for

    Returns:
        An (ok, error message) tuple.
    """
    release_path = options.dest_root / tag.dest
    working_path = options.working_root / tag.dest
    previous_path = options.previous_root / tag.dest
    try:
        os.makedirs(working_path, exist_ok=True)
    except OSError as err:
        msg = f"OSError creating working dir {working_path}, {err}"
        _log.error("%s", msg, exc_info=_debug)
        return False, msg

    # Set up the lock file
    lock_fh = None
    lock_path = ""
    if options.lock_dir:
        lock_path = options.lock_dir / tag.name
        try:
            os.makedirs(options.lock_dir, exist_ok=True)
            lock_fh = acquire_lock(lock_path)
        except OSError as err:
            msg = f"OSError creating lockfile at {lock_path}, {err}"
            _log.error("%s", msg, exc_info=_debug)
            return False, msg
        if not lock_fh:
            msg = f"Another run in progress (unable to lock file {lock_path})"
            _log.error("%s", msg)
            return False, msg

    # Run the various steps
    try:
        latest_dir = get_koji_latest_dir(options.koji_rsync, tag.source)
        source_url = f"{options.koji_rsync}/{tag.source}/{latest_dir}/"
        rsync_from_koji(
            source_url=source_url, dest_path=working_path, link_path=release_path
        )
        pull_condor_repos(options, tag)
        update_pkglist_files(working_path, tag.arches)
        run_createrepo(working_path, tag.arches)
        create_compat_symlink(working_path)
        update_release_repos(
            release_path=release_path,
            working_path=working_path,
            previous_path=previous_path,
        )
    except TagFailure as err:
        _log.error("Tag %s failed: %s", tag.name, err, exc_info=_debug)
        return False, str(err)
    finally:
        # Release the lock
        if lock_fh:
            try:
                release_lock(lock_fh, lock_path)
            except OSError as err:
                _log.warning("OSError releasing lock file at %s: %s", lock_path, err)
    return True, ""
