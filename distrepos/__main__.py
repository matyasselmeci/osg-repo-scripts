#!/usr/bin/env python3
"""
This script rsyncs repos created with "koji dist-repo" and combines them with
other external repos (such as the htcondor repo), then updates the repo
definition files.  The list of repositories is pulled from a config file.

The mash-created repo layout looks like
    source/SRPMS/{*.src.rpm,repodata/,repoview/}
    x86_64/{*.rpm,repodata/,repoview/}
    x86_64/debug/{*-{debuginfo,debugsource}*.rpm,repodata/,repoview/}

The distrepo layout looks like (where <X> is the first letter of the package name)
    src/repodata/
    src/pkglist
    src/Packages/<X>/*.src.rpm
    x86_64/repodata/
    x86_64/pkglist
    x86_64/debug/pkglist
    x86_64/debug/repodata/
    x86_64/Packages/<X>/{*.rpm, *-{debuginfo,debugsource}*.rpm}

Note that the debuginfo and debugsource rpm files are mixed in with the regular files.
The "pkglist" files are text files listing the relative paths to the packages in the
repo -- this is passed to `createrepo` to put the debuginfo and debugsource RPMs into
separate repositories even though the files are mixed together.
"""

import configparser
import fcntl
import fnmatch
import logging
import logging.handlers
import os
import re
import shutil
import string
import subprocess as sp
import sys
import tempfile
import typing as t
from argparse import ArgumentParser, Namespace, RawDescriptionHelpFormatter
from configparser import ConfigParser, ExtendedInterpolation
from pathlib import Path

MB = 1 << 20
LOG_MAX_SIZE = 500 * MB

ERR_CONFIG = 3
ERR_RSYNC = 4
ERR_FAILURES = 5
ERR_EMPTY = 6

RSYNC_OK = 0
RSYNC_NOT_FOUND = 23

DEFAULT_CONFIG = "/etc/distrepos.conf"
DEFAULT_CONDOR_RSYNC = "rsync://rsync.cs.wisc.edu/htcondor"
DEFAULT_KOJI_RSYNC = "rsync://kojihub2000.chtc.wisc.edu/repos-dist"
DEFAULT_DESTROOT = "/usr/local/repo"
DEFAULT_LOCK_DIR = "/var/lock/rsync_dist_repo"

# These options are required to be present _and_ nonempty.  Some of them may
# come from the DEFAULT section.
REQUIRED_TAG_OPTIONS = ["dest", "arches", "arch_rpms_subdir", "source_rpms_subdir"]

_debug = False
_log = logging.getLogger(__name__)


#
# Error classes
#


class ProgramError(RuntimeError):
    """
    Class for fatal errors during execution.  The `returncode` parameter
    should be used as the exit code for the program.
    """

    def __init__(self, returncode, *args):
        super().__init__(*args)
        self.returncode = returncode


class ConfigError(ProgramError):
    """Class for errors with the configuration"""

    def __init__(self, *args):
        super().__init__(ERR_CONFIG, *args)

    def __str__(self):
        return f"Config error: {super().__str__()}"


class MissingOptionError(ConfigError):
    """Class for missing a required option in a config section"""

    def __init__(self, section_name: str, option_name: str):
        super().__init__(
            f"Section [{section_name}] missing or empty required option {option_name}"
        )


class TagFailure(Exception):
    """
    Class for failure for a specific tag.  Not meant to be fatal.
    """


#
# Data classes
#


class SrcDst(t.NamedTuple):
    """A source/destination pair"""

    src: str
    dst: str

    def __str__(self):
        return f"{self.src} -> {self.dst}"


class Tag(t.NamedTuple):
    name: str
    source: str
    dest: str
    arches: t.List[str]
    condor_repos: t.List[SrcDst]
    arch_rpms_dest: str
    debug_rpms_dest: str
    source_rpms_dest: str


class Options(t.NamedTuple):
    # TODO docstring
    dest_root: Path
    working_root: Path
    previous_root: Path
    koji_rsync: str
    condor_rsync: str
    lock_dir: t.Optional[Path]
    mirror_root: t.Optional[Path]
    mirror_hosts: t.List[str]
    make_repoview: bool = False


#
# Misc helpful functions
#
def acquire_lock(lock_path: t.Union[str, os.PathLike]) -> t.Optional[t.IO]:
    """
    Create and return the handle to a lockfile

    Args:
        lock_path: The path to the lockfile to create; the directory must
            already exist.

    Returns: A filehandle to be used with release_lock(), or None if we were
        unable to acquire the lock.
    """
    filehandle = open(lock_path, "w")
    filedescriptor = filehandle.fileno()
    # Get an exclusive lock on the file (LOCK_EX) in non-blocking mode
    # (LOCK_NB), which causes the operation to raise IOError if some other
    # process already has the lock
    try:
        fcntl.flock(filedescriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        return None
    return filehandle


def release_lock(lock_fh: t.Optional[t.IO], lock_path: t.Optional[str]):
    """
    Release a lockfile created with acquire_lock()
    Args:
        lock_fh: The filehandle created by acquire_lock()
        lock_path: The path to the lockfile to delete
    """
    if not lock_fh:
        return  # no lock; do nothing
    filedescriptor = lock_fh.fileno()
    fcntl.flock(filedescriptor, fcntl.LOCK_UN)
    lock_fh.close()
    if lock_path:
        os.unlink(lock_path)


def _log_ml(lvl: int, msg: str, *args, **kwargs):
    """
    Log a potentially multi-line message by splitting the lines and doing
    individual calls to _log.log().  exc_info and stack_info will only be
    printed for the last line.
    """
    if lvl >= _log.getEffectiveLevel():
        orig_kwargs = kwargs.copy()
        msg_lines = (msg % args).splitlines()
        last_line = msg_lines[-1]
        kwargs.pop("exc_info", None)
        kwargs.pop("stack_info", None)
        for line in msg_lines[:-1]:
            _log.log(lvl, "%s", line, **kwargs)
        return _log.log(lvl, "%s", last_line, **orig_kwargs)


def format_tag(
    tag: Tag, koji_rsync: str, condor_rsync: str, destroot: t.Union[os.PathLike, str]
) -> str:
    """
    Return the pretty-printed parsed information for a tag we are going to copy.

    Args:
        tag: the Tag object to print the information for
        koji_rsync: the base rsync URL for the Koji distrepos
        condor_rsync: the rsync URL for the Condor repos
        destroot: the local directory that files will be rsynced to

    Returns: the formatted text as a string
    """
    arches_str = " ".join(tag.arches)
    ret = f"""\
Tag {tag.name}
source           : {koji_rsync}/{tag.source}
dest             : {destroot}/{tag.dest}
arches           : {arches_str}
arch_rpms_dest   : {destroot}/{tag.arch_rpms_dest}
debug_rpms_dest  : {destroot}/{tag.debug_rpms_dest}
source_rpms_dest : {destroot}/{tag.source_rpms_dest}
"""
    if tag.condor_repos:
        joiner = "\n" + 19 * " " + f"{condor_rsync}/"
        condor_repos_str = f"{condor_rsync}/" + joiner.join(
            str(it) for it in tag.condor_repos
        )
        ret += f"""\
condor_repos     : {condor_repos_str}
"""
    return ret


#
# Wrappers around process handling and logging
#


def ellipsize_lines(lines: t.Sequence[str], max_lines: int) -> t.List[str]:
    """
    If the given list of lines is longer than max_lines, replace the middle
    with a single "..." line.

    As a special case, return [] on None or any other false-ish value.
    """
    if not lines:
        return []
    if isinstance(lines, str):
        lines = lines.splitlines()
    half_max_lines = max_lines // 2
    if len(lines) > max_lines:
        return lines[:half_max_lines] + ["..."] + lines[-half_max_lines:]
    else:
        return lines


def log_proc(
    proc: t.Union[sp.CompletedProcess, sp.CalledProcessError],
    description: str = None,
    ok_exit: t.Union[int, t.Container[int]] = 0,
    success_level=logging.DEBUG,
    failure_level=logging.ERROR,
    stdout_max_lines=24,
    stderr_max_lines=40,
) -> None:
    """
    Print the result of a process in the log; the loglevel is determined by
    success or failure. stdout/stderr are ellipsized if too long.

    Args:
        proc: The result of running a process
        description: An optional description of what we tried to do by
            launching the process
        ok_exit: One or more exit codes that are considered not failures
        success_level: The loglevel for printing stdout/stderr on success
        failure_level: The loglevel for printing stdout/stderr on failure
        stdout_max_lines: The maximum number of lines of stdout to print before
            ellipsizing
        stderr_max_lines: The maximum number of lines of stderr to print before
            ellipsizing
    """

    if isinstance(ok_exit, int):
        ok_exit = [ok_exit]
    ok = proc.returncode in ok_exit
    level = success_level if ok else failure_level
    if not description:
        if isinstance(proc, sp.CompletedProcess):
            description = proc.args[0]
        elif isinstance(proc, sp.CalledProcessError):
            description = proc.cmd[0]
        else:  # bad typing but let's deal with it anyway
            description = "process"
    outerr = []
    if proc.stdout:
        outerr += ["-----", "Stdout:"] + ellipsize_lines(proc.stdout, stdout_max_lines)
    if proc.stderr:
        outerr += ["-----", "Stderr:"] + ellipsize_lines(proc.stderr, stderr_max_lines)
    outerr += ["-----"]
    outerr_s = "\n".join(outerr)
    _log.log(
        level,
        f"%s %s with exit code %d\n%s",
        description,
        "succeeded" if ok else "failed",
        proc.returncode,
        outerr_s,
    )


def run_with_log(
    *args,
    ok_exit: t.Union[int, t.Container[int]] = 0,
    success_level=logging.DEBUG,
    failure_level=logging.ERROR,
    stdout_max_lines=24,
    stderr_max_lines=40,
    **kwargs,
) -> t.Tuple[bool, sp.CompletedProcess]:
    """
    Helper function to run a command and log its output.  Returns a boolean of
    whether the exit code was acceptable, and the CompletedProcess object.

    See Also: log_proc()
    """
    if isinstance(ok_exit, int):
        ok_exit = [ok_exit]
    kwargs.setdefault("stdout", sp.PIPE)
    kwargs.setdefault("stderr", sp.PIPE)
    kwargs.setdefault("encoding", "latin-1")
    if _debug:
        _log.debug("running %r %r", args, kwargs)
    proc = sp.run(*args, **kwargs)
    ok = proc.returncode in ok_exit
    log_proc(
        proc,
        args[0],
        ok_exit,
        success_level,
        failure_level,
        stdout_max_lines,
        stderr_max_lines,
    )
    return ok, proc


#
# Wrappers around rsync
#


def rsync(*args, **kwargs) -> t.Tuple[bool, sp.CompletedProcess]:
    """
    A wrapper around `subprocess.run` that runs rsync, capturing the output
    and error, printing the command to be run if we're in debug mode.
    Returns an (ok, CompletedProcess) tuple where ok is True if the return code is 0.
    """
    kwargs.setdefault("stdout", sp.PIPE)
    kwargs.setdefault("stderr", sp.PIPE)
    kwargs.setdefault("encoding", "latin-1")
    cmd = ["rsync"] + [str(x) for x in args]
    if _debug:
        _log.debug("running %r %r", cmd, kwargs)
    try:
        proc = sp.run(cmd, **kwargs)
    except OSError as err:
        # This is usually caused by something like rsync not being found
        raise ProgramError(ERR_RSYNC, f"Invoking rsync failed: {err}") from err
    return proc.returncode == 0, proc


def rsync_with_link(
    source_url: str,
    dest_path: t.Union[str, os.PathLike],
    link_path: t.Union[None, str, os.PathLike],
    recursive=True,
    delete=True,
) -> t.Tuple[bool, sp.CompletedProcess]:
    """
    rsync from a remote URL sourcepath to the destination destpath, optionally
    linking to files in linkpath.  recursive by default but this can be turned
    off.
    """
    args = [
        "--times",
        "--stats",
    ]
    if delete:
        args.append("--delete")
    if recursive:
        args.append("--recursive")
    elif delete:
        # rsync --delete errors out if neither --recursive nor --dirs are specified
        args.append("--dirs")
    if link_path and os.path.exists(link_path):
        args.append(f"--link-dest={link_path}")
    args += [
        source_url,
        dest_path,
    ]
    return rsync(*args)


def log_rsync(
    proc: sp.CompletedProcess,
    description: str = "rsync",
    success_level=logging.DEBUG,
    failure_level=logging.ERROR,
    not_found_is_ok=False,
):
    """
    log the result of an rsync() call.  The log level and message are based on
    its success or failure (i.e., returncode == 0).  If not_found_is_ok is True,
    then a source file not found (returncode == 23) is also considered ok.
    """
    ok_exit = [RSYNC_OK]
    if not_found_is_ok:
        ok_exit.append(RSYNC_NOT_FOUND)
    return log_proc(
        proc,
        description=description,
        ok_exit=ok_exit,
        success_level=success_level,
        failure_level=failure_level,
    )


def check_rsync(koji_rsync: str):
    """
    Run an rsync listing of the rsync root. If this fails, there is no point
    in proceeding further.
    """
    description = f"koji-hub rsync endpoint {koji_rsync} directory listing"
    try:
        ok, proc = rsync("--list-only", koji_rsync, timeout=180)
    except sp.TimeoutExpired:
        _log.critical(f"{description} timed out")
        raise ProgramError(
            ERR_RSYNC, "rsync dir listing from koji-hub timed out, cannot continue"
        )
    log_rsync(
        proc,
        description,
        failure_level=logging.CRITICAL,
    )
    if not ok:
        raise ProgramError(
            ERR_RSYNC, "rsync dir listing from koji-hub failed, cannot continue"
        )


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


def run_repoview(working_path: Path, arches: t.List[str]):
    _log.debug("run_repoview(%r, %r)", working_path, arches)
    raise NotImplementedError()


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


#
# The overall task runners
#


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
        if options.make_repoview:
            run_repoview(working_path, tag.arches)
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


#
# Functions for dealing with the mirror list
#


def create_mirrorlists(options: Options, tags: t.Sequence[Tag]) -> t.Tuple[bool, str]:
    """
    Create the files used for mirror lists

    Args:
        options: The global options for the run
        tags: The list of tags to create mirror lists for

    Returns:
        A (success, message) tuple where success is True or False, and message
        describes the particular failure.
    """
    # Set up the lock file
    lock_fh = None
    lock_path = ""
    if options.lock_dir:
        lock_path = options.lock_dir / "mirrors"
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

    try:
        pass
    finally:
        release_lock(lock_fh, lock_path)


#
# Functions for handling command-line arguments and config
#


def match_globlist(text: str, globlist: t.List[str]) -> bool:
    """
    Return True if `text` matches one of the globs in globlist.
    """
    return any(fnmatch.fnmatch(text, g) for g in globlist)


def get_source_dest_opt(option: str) -> t.List[SrcDst]:
    """
    Parse a config option of the form
        SRC1 -> DST1
        SRC2 -> DST2
    Returning a list of SrcDst objects.
    Blank lines are ignored.
    Leading and trailing whitespace and slashes are stripped.
    A warning is emitted for invalid lines.
    """
    ret = []
    for line in option.splitlines():
        line = line.strip()
        if not line:
            continue
        mm = re.fullmatch(r"(.+?)\s*->\s*(.+?)", line)
        if mm:
            ret.append(SrcDst(mm.group(1).strip("/"), mm.group(2).strip("/")))
        else:
            _log.warning("Skipping invalid source->dest line %r", line)
    return ret


def get_args(argv: t.List[str]) -> Namespace:
    """
    Parse command-line arguments
    """
    parser = ArgumentParser(
        prog=argv[0], description=__doc__, formatter_class=RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--config",
        default=DEFAULT_CONFIG,
        help="Config file to pull tag and repository information from. Default: %(default)s",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Output debug messages",
    )
    # parser.add_argument(
    #     "--no-debug",
    #     dest="debug",
    #     action="store_false",
    #     help="Do not output debug messages",
    # )
    parser.add_argument(
        "--logfile",
        default="",
        help="Logfile to write output to (no default)",
    )
    parser.add_argument(
        "--destroot",
        default="",
        help="Top of destination directory; individual repos will be placed "
        "relative to this directory. Default: %s" % DEFAULT_DESTROOT,
    )
    parser.add_argument(
        "--lock-dir",
        default=DEFAULT_LOCK_DIR,
        help="Directory to create locks in to simultaneous writes for the same tag. "
        "Set to empty to disable locking. Default: %(default)s",
    )
    parser.add_argument(
        "--tag",
        action="append",
        dest="tags",
        default=[],
        help="Tag to pull. Default is all the tags in the config. "
        "Can be specified multiple times. Can be a glob.",
    )
    parser.add_argument(
        "--print-tags",
        action="store_true",
        help="Don't run, just print the parsed tag definitions to stdout.",
    )
    args = parser.parse_args(argv[1:])
    return args


def setup_logging(args: Namespace, config: ConfigParser) -> None:
    """
    Sets up logging, given the config and the command-line arguments.

    Logs are written to a logfile if one is defined. In addition,
    log to stderr if it's a tty.
    """
    loglevel = logging.DEBUG if _debug else logging.INFO
    _log.setLevel(loglevel)
    if sys.stderr.isatty():
        ch = logging.StreamHandler()
        ch.setLevel(loglevel)
        chformatter = logging.Formatter(">>>\t%(message)s")
        ch.setFormatter(chformatter)
        _log.addHandler(ch)
    if args.logfile:
        logfile = args.logfile
    else:
        logfile = config.get("options", "logfile", fallback="")
    if logfile:
        rfh = logging.handlers.RotatingFileHandler(
            logfile,
            maxBytes=LOG_MAX_SIZE,
            backupCount=1,
        )
        rfh.setLevel(loglevel)
        rfhformatter = logging.Formatter(
            "%(asctime)s - %(levelname)s - %(message)s",
        )
        rfh.setFormatter(rfhformatter)
        _log.addHandler(rfh)


def _expand_tagset(config: ConfigParser, tagset_section_name: str):
    """
    Expand a 'tagset' section into multiple 'tag' sections, substituting each
    value of the tagset's 'dvers' option into "$${EL}".
    Modifies 'config' in-place.
    """
    if "${EL}" not in tagset_section_name and "$EL" not in tagset_section_name:
        raise ConfigError(
            f"Section name [{tagset_section_name}] does not contain '${{EL}}'"
        )
    tagset_section = config[tagset_section_name]
    tagset_name = tagset_section_name.split(" ", 1)[1].strip()

    # Check for the option we're supposed to be looping over
    if not tagset_section.get("dvers"):
        raise MissingOptionError(tagset_section_name, "dvers")

    # Also check for the options that are supposed to be in the 'tag' sections, otherwise
    # we'd get some confusing error messages when we get to parsing those.
    for opt in REQUIRED_TAG_OPTIONS:
        if not tagset_section.get(opt):
            raise MissingOptionError(tagset_section_name, opt)

    def sub_el(a_string):
        """Substitute the value of `dver` for $EL"""
        return string.Template(a_string).safe_substitute({"EL": dver})

    # Loop over the dvers, expand into tag sections
    for dver in tagset_section["dvers"].split():
        tag_name = sub_el(
            tagset_name.replace(
                "$$", "$"
            )  # ConfigParser does not interpolate in section names
        )
        tag_section_name = f"tag {tag_name}"
        try:
            config.add_section(tag_section_name)
            # _log.debug(
            #     "Created section [%s] from [%s]", tag_section_name, tagset_section_name
            # )
        except configparser.DuplicateSectionError:
            _log.debug(
                "Skipping section [%s] because it already exists", tag_section_name
            )
            continue
        for key in tagset_section:
            if key == "dvers":
                continue
            try:
                value = tagset_section.get(key, raw=False)
            except configparser.InterpolationError:
                value = tagset_section.get(key, raw=True)
            new_value = sub_el(value)
            # _log.debug("Setting {%s:%s} to %r", tag_section_name, key, new_value)
            config[tag_section_name][key] = new_value.replace("$", "$$")
            # ^^ escaping the $'s again because we're doing another round of
            # interpolation when we read the 'tag' sections created from this


def _get_taglist_from_config(
    config: ConfigParser, tagnames: t.List[str]
) -> t.List[Tag]:
    """
    Parse the 'tag' and 'tagset' sections in the config to return a list of Tag objects.
    This calls _expand_tagset to expand tagset sections, which may modify the config object.
    If 'tagnames' is nonempty, limits the tags to only those named in tagnames.
    """
    taglist = []

    # First process tagsets; this needs to be in a separate loop because it creates
    # tag sections.
    for tagset_section_name in (
        x for x in config.sections() if x.lower().startswith("tagset ")
    ):
        _expand_tagset(config, tagset_section_name)

    # Now process the tag sections.
    for section_name, section in config.items():
        if not section_name.lower().startswith("tag "):
            continue

        tag_name = section_name.split(" ", 1)[1].strip()
        if tagnames and not match_globlist(tag_name, tagnames):
            continue
        source = section.get("source", tag_name)

        for opt in REQUIRED_TAG_OPTIONS:
            if not section.get(opt):
                raise MissingOptionError(section_name, opt)

        dest = section["dest"].strip("/")
        arches = section["arches"].split()
        condor_repos = get_source_dest_opt(section.get("condor_repos", ""))
        arch_rpms_subdir = section["arch_rpms_subdir"].strip("/")
        debug_rpms_subdir = section.get(
            "debug_rpms_subdir", fallback=arch_rpms_subdir
        ).strip("/")
        source_rpms_subdir = section["source_rpms_subdir"].strip("/")
        taglist.append(
            Tag(
                name=tag_name,
                source=source,
                dest=dest,
                arches=arches,
                condor_repos=condor_repos,
                arch_rpms_dest=f"{dest}/{arch_rpms_subdir}",
                debug_rpms_dest=f"{dest}/{debug_rpms_subdir}",
                source_rpms_dest=f"{dest}/{source_rpms_subdir}",
            )
        )

    return taglist


def parse_config(
    args: Namespace, config: ConfigParser
) -> t.Tuple[Options, t.List[Tag]]:
    """
    Parse the config file and return the Distrepos object from the parameters.
    Apply any overrides from the command-line.
    """
    taglist = _get_taglist_from_config(config, args.tags)
    if not taglist:
        raise ConfigError("No (matching) [tag ...] or [tagset ...] sections found")

    if "options" not in config:
        raise ConfigError("Missing required section [options]")
    options_section = config["options"]
    if args.destroot:
        dest_root = args.destroot.rstrip("/")
        working_root = dest_root + ".working"
        previous_root = dest_root + ".previous"
    else:
        dest_root = options_section.get("dest_root", DEFAULT_DESTROOT).rstrip("/")
        working_root = options_section.get("working_root", dest_root + ".working")
        previous_root = options_section.get("previous_root", dest_root + ".previous")
    mirror_root = options_section.get("mirror_root", None)
    mirror_hosts = options_section.get("mirror_hosts", "").split()
    return (
        Options(
            dest_root=Path(dest_root),
            working_root=Path(working_root),
            previous_root=Path(previous_root),
            condor_rsync=options_section.get("condor_rsync", DEFAULT_CONDOR_RSYNC),
            koji_rsync=options_section.get("koji_rsync", DEFAULT_KOJI_RSYNC),
            lock_dir=Path(args.lock_dir) if args.lock_dir else None,
            mirror_root=mirror_root,
            mirror_hosts=mirror_hosts,
        ),
        taglist,
    )


#
# Main function
#


def main(argv: t.Optional[t.List[str]] = None) -> int:
    """
    Main function. Parse arguments and config; set up logging and the parameters
    for each run, then launch the run.

    Return the exit code of the program.  Success (0) is if at least one tag succeeded
    and no tags failed.
    """
    global _debug

    args = get_args(argv or sys.argv)
    config_path: str = args.config
    config = ConfigParser(interpolation=ExtendedInterpolation())
    config.read(config_path)

    if args.debug:
        _debug = True
    else:
        try:
            _debug = config.getboolean("options", "debug")
        except configparser.Error:
            _debug = False

    setup_logging(args, config)
    options, taglist = parse_config(args, config)

    if args.print_tags:
        for tag in taglist:
            print(
                format_tag(
                    tag,
                    koji_rsync=options.koji_rsync,
                    condor_rsync=options.condor_rsync,
                    destroot=options.dest_root,
                )
            )
            print("------")
        return 0

    _log.info("Program started")
    check_rsync(options.koji_rsync)
    _log.info("rsync check successful. Starting run for %d tags", len(taglist))

    successful = []
    failed = []
    for tag in taglist:
        _log.info("----------------------------------------")
        _log.info("Starting tag %s", tag.name)
        _log_ml(
            logging.DEBUG,
            "%s",
            format_tag(
                tag,
                koji_rsync=options.koji_rsync,
                condor_rsync=options.condor_rsync,
                destroot=options.dest_root,
            ),
        )
        ok, err = run_one_tag(options, tag)
        if ok:
            _log.info("Tag %s completed", tag.name)
            successful.append(tag)
        else:
            _log.error("Tag %s failed", tag.name)
            failed.append((tag, err))

    _log.info("----------------------------------------")
    _log.info("Run completed")

    # Report on the results
    successful_names = [it.name for it in successful]
    if successful:
        _log_ml(
            logging.INFO,
            "%d tags succeeded:\n  %s",
            len(successful_names),
            "\n  ".join(successful_names),
        )
    if failed:
        _log.error("%d tags failed:", len(failed))
        for tag, err in failed:
            _log.error("  %-40s: %s", tag.name, err)
        return ERR_FAILURES
    elif not successful:
        _log.error("No tags were pulled")
        return ERR_EMPTY

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except ProgramError as e:
        _log.error("%s", e, exc_info=_debug)
        sys.exit(e.returncode)
