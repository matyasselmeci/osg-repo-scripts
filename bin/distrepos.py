#!/usr/bin/env python3
"""
This script rsyncs repos created with "koji dist-repo" and combines them with
other external repos (such as the htcondor repo), then updates the repo
definition files.  The list of repositories is pulled from a config file.
"""

import configparser
import tempfile
from configparser import ConfigParser, ExtendedInterpolation
import logging
import logging.handlers
import os
import re
import shutil
import subprocess as sp
import sys
import typing as t
from argparse import ArgumentParser, Namespace
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
DEFAULT_DESTROOT = "/var/www/repo"

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


class Tag(t.NamedTuple):
    name: str
    source: str
    dest: str
    arches: t.List[str]
    condor_repos: t.List[SrcDst]
    arch_rpms_dest: str
    debug_rpms_dest: str
    source_rpms_dest: str


#
# Other functions
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


def rsync_exists(remote_url: str) -> bool:
    ok, proc = rsync(["--list-only", remote_url])
    if proc.returncode == 0:
        return True
    elif proc.returncode == 23:
        return False
    else:
        pass  # XXX now what?


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
        args.append(f"--link-path={link_path}")
    args += [
        source_url,
        dest_path,
    ]
    return rsync(*args)


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
    not_found = proc.returncode == RSYNC_NOT_FOUND
    ok = proc.returncode == RSYNC_OK
    stdout = "\n".join(ellipsize_lines(proc.stdout, 24))
    stderr = "\n".join(ellipsize_lines(proc.stderr, 40))
    outerr = f"Stdout:\n{stdout}\n\nStderr:\n{stderr}\n------"
    if ok:
        _log.log(
            success_level,
            "%s succeeded\n%s",
            description,
            outerr,
        )
    elif not_found and not_found_is_ok:
        _log.log(
            success_level,
            "%s did not find source\n%s",
            description,
            outerr,
        )
    else:
        _log.log(
            failure_level,
            "%s failed with exit code %d\n%s",
            description,
            proc.returncode,
            outerr,
        )


#
# Main Distrepos class
#


class Distrepos:
    """
    The Distrepos class contains the parameters and code for one run of repo updates
    of all tags in the config.

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

    def __init__(
        self,
        dest_root: t.Union[os.PathLike, str],
        working_root: t.Union[os.PathLike, str],
        previous_root: t.Union[os.PathLike, str],
        koji_rsync: str,
        condor_rsync: str,
        taglist: t.List[Tag],
    ):
        self.taglist = taglist
        self.dest_root = Path(dest_root)
        self.working_root = Path(working_root)
        self.previous_root = Path(previous_root)

        self.koji_rsync = koji_rsync
        self.condor_rsync = condor_rsync
        self.srpm_compat_symlink = True  # XXX should this be configurable? per tag?
        self.make_repoview = False  # XXX should be configurable

    def check_rsync(self):
        """
        Run an rsync listing of the rsync root. If this fails, there is no point
        in proceeding further.
        """
        description = f"koji-hub rsync endpoint {self.koji_rsync} directory listing"
        try:
            ok, proc = rsync("--list-only", self.koji_rsync, timeout=180)
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

    def run(self) -> t.Tuple[t.List[Tag], t.List[Tag]]:
        """
        Run the sequence on all the tags; return a list of successful tags and a list of failed tags.
        """
        successful = []
        failed = []
        for tag in self.taglist:
            if self.run_one_tag(tag):
                successful.append(tag)
            else:
                failed.append(tag)
        return successful, failed

    def run_one_tag(self, tag: Tag) -> bool:
        """
        Run all the actions necessary to create a repo for one tag in the config.
        Return True on success or False on failure.
        """
        release_path = self.dest_root / tag.dest
        working_path = self.working_root / tag.dest
        previous_path = self.previous_root / tag.dest
        try:
            os.makedirs(working_path, exist_ok=True)
        except OSError as err:
            _log.error(
                "OSError creating working dir %s: %s",
                working_path,
                err,
                exc_info=_debug,
            )
            return False
        latest_dir = self._get_latest_dir(tag.source)
        source_url = f"{self.koji_rsync}/{tag.source}/{latest_dir}/"
        try:
            self._rsync_one_tag(
                source_url=source_url, dest_path=working_path, link_path=release_path
            )
            self._pull_condor_repos(tag)
            self._update_pkglist_files(working_path, tag.arches)
            self._run_createrepo(working_path, tag.arches)
            self._run_repoview(working_path, tag.arches)
            self._create_compat_symlink(working_path)
            self._update_release_repos(
                release_path=release_path,
                working_path=working_path,
                previous_path=previous_path,
            )
        except TagFailure as err:
            _log.error("Tag %s failed: %s", tag.name, err, exc_info=_debug)
            return False
        return True

    def _get_latest_dir(self, tagdir: str) -> str:
        """
        Resolves the "latest" symlink for the dist-repo on koji-hub by downloading
        the symlink to a temporary directory and reading it.  (We don't want to use
        the "latest" symlink directly since it may change mid-run.)
        """
        with tempfile.TemporaryDirectory() as tempdir:
            destpath = os.path.join(tempdir, "latest")
            try:
                ok, proc = rsync(
                    "-l", f"{self.koji_rsync}/{tagdir}/latest", destpath, timeout=180
                )
            except sp.TimeoutExpired:
                raise TagFailure("Timeout getting 'latest' dir")
            log_rsync(proc, "Getting 'latest' dir symlink")
            if not ok:
                raise TagFailure("Error getting 'latest' dir")
            # we have copied the "latest" symlink as a (now broken) symlink. Read the text of the link to get
            # the directory on the remote side.
            return os.path.basename(os.readlink(destpath))

    @staticmethod
    def _rsync_one_tag(source_url, dest_path, link_path):
        """
        rsync the distrepo from kojihub for one tag, linking to the RPMs in
        the previous repo if they exist
        """
        _log.debug("_rsync_one_tag(%r, %r, %r)", source_url, dest_path, link_path)
        ok, proc = rsync_with_link(source_url, dest_path, link_path)
        log_rsync(proc, f"rsync from {source_url} to {dest_path}")
        if not ok:
            raise TagFailure(f"Error rsyncing {source_url} to {dest_path}")

    def _pull_condor_repos(self, tag: Tag):
        """
        rsync binary and source RPMs from condor repos defined for this tag.
        """
        _log.debug("_pull_condor_repos(%r)", tag)
        # Condor SRPMS are in a subdirectory of the arch-specific condor-directory.
        # We do not do a recursive rsync because we prefer to put the SRPMS elsewhere.

        for idx, arch in enumerate(tag.arches):
            for repo in tag.condor_repos:
                arch_rpms_src = f"{self.condor_rsync}/{repo.src}/".replace(
                    "%{ARCH}", arch
                )
                debug_rpms_src = arch_rpms_src + "debug/"
                source_rpms_src = arch_rpms_src + "SRPMS/"

                arch_rpms_dst = (
                    f"{self.working_root}/{tag.arch_rpms_dest}/{repo.dst}/".replace(
                        "%{ARCH}", arch
                    )
                )
                debug_rpms_dst = (
                    f"{self.working_root}/{tag.debug_rpms_dest}/{repo.dst}/".replace(
                        "%{ARCH}", arch
                    )
                )
                source_rpms_dst = (
                    f"{self.working_root}/{tag.source_rpms_dest}/{repo.dst}/".replace(
                        "%{ARCH}", arch
                    )
                )

                arch_rpms_link = (
                    f"{self.dest_root}/{tag.arch_rpms_dest}/{repo.dst}/".replace(
                        "%{ARCH}", arch
                    )
                )
                debug_rpms_link = (
                    f"{self.dest_root}/{tag.debug_rpms_dest}/{repo.dst}/".replace(
                        "%{ARCH}", arch
                    )
                )
                source_rpms_link = (
                    f"{self.dest_root}/{tag.source_rpms_dest}/{repo.dst}/".replace(
                        "%{ARCH}", arch
                    )
                )

                # First, pull the main (binary) RPMs
                description = f"rsync from condor repo for {arch} RPMs"
                ok, proc = rsync_with_link(
                    arch_rpms_src,
                    arch_rpms_dst,
                    arch_rpms_link,
                    delete=False,
                    recursive=False,
                )
                log_rsync(proc, description)
                if not ok:
                    raise TagFailure(f"Error pulling condor repos: {description}")

                # Next pull the debuginfo RPMs.  These may not exist.
                description = f"rsync from condor repo for {arch} debug RPMs"
                _, proc = rsync_with_link(
                    debug_rpms_src,
                    debug_rpms_dst,
                    debug_rpms_link,
                    delete=False,
                    recursive=False,
                )
                log_rsync(proc, description, not_found_is_ok=True)
                if proc.returncode not in {RSYNC_OK, RSYNC_NOT_FOUND}:
                    raise TagFailure(f"Error pulling condor repos: {description}")

                # Finally pull the SRPMs -- these are identical between arches so only
                # pull if we're on the first arch.
                if idx == 0:
                    description = f"rsync from condor repo for source RPMs"
                    ok, proc = rsync_with_link(
                        source_rpms_src,
                        source_rpms_dst,
                        source_rpms_link,
                        delete=False,
                        recursive=False,
                    )
                    log_rsync(proc, description)
                    if not ok:
                        raise TagFailure(f"Error pulling condor repos: {description}")

    def _update_pkglist_files(self, working_path: Path, arches: t.List[str]):
        """
        Update the "pkglist" files with the relative paths of the RPM files, including
        files that were pulled from the condor repos.  Put debuginfo files in a separate
        pkglist.
        """
        _log.debug("_update_pkglist_files(%r, %r)", working_path, arches)
        # Update pkglist files for SRPMs.  There's no such thing as a debuginfo SRPM so
        # we don't have to handle those.
        src_dir = working_path / "src"
        src_pkglist = src_dir / "pkglist"
        src_packages_dir = src_dir / "Packages"
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
        except OSError as err:
            raise TagFailure(
                f"OSError updating pkglist file {src_pkglist}: {err}"
            ) from err

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
                shutil.move(f"{arch_debug_pkglist}.new", arch_debug_pkglist)
            except OSError as err:
                raise TagFailure(
                    f"OSError updating pkglist files {arch_pkglist} and {arch_debug_pkglist}: {err}"
                ) from err

    def _run_createrepo(self, working_path: Path, arches: t.List[str]):
        raise NotImplementedError()

    def _run_repoview(self, working_path: Path, arches: t.List[str]):
        if self.make_repoview:
            raise NotImplementedError()
        _ = working_path
        _ = arches

    def _create_compat_symlink(self, working_path: Path):
        """
        Create a symlink from
            <repo>/source/SRPMS (mash layout) -> <repo>/src (distrepo layout)
        """
        _log.debug("_create_compat_symlink(%r, %r)", working_path)
        try:
            (working_path / "source").mkdir(parents=True, exist_ok=True)
            if (working_path / "source/SRPMS").exists():
                shutil.rmtree(working_path / "source/SRPMS")
            os.symlink(working_path / "src", working_path / "source/SRPMS")
        except OSError as err:
            raise TagFailure("Error creating SRPM compat symlink") from err

    def _update_release_repos(
        self, release_path: Path, working_path: Path, previous_path: Path
    ):
        """
        Update the published repos by moving the published dir to the 'previous' dir
        and the working dir to the published dir.
        """
        _log.debug(
            "_update_release_repos(%r, %r, %r)",
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


#
# Functions for handling command-line arguments and config
#


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
    parser = ArgumentParser(prog=argv[0], description=__doc__)
    parser.add_argument(
        "--config",
        default=DEFAULT_CONFIG,
        help="Config file to pull tag and repository information from. Default: %(default)s",
    )
    # parser.add_argument(
    #     "--debug",
    #     action="store_true",
    #     help="Output debug messages",
    # )
    parser.add_argument(
        "--no-debug",
        dest="debug",
        action="store_false",
        help="Do not output debug messages",
    )
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
        "--tag",
        action="append",
        dest="tags",
        default=[],
        help="Tag to pull. Default is all the tags in the config. Can be specified multiple times."
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
        chformatter = logging.Formatter("%(message)s")
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
    value of the tagset's 'dvers' option into "%{EL}".
    Modifies 'config' in-place.
    """
    if "%{EL}" not in tagset_section_name:
        raise ConfigError(
            f"Section name [{tagset_section_name}] does not contain '%{{EL}}'"
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

    # Loop over the dvers, expand into tag sections
    for dver in tagset_section["dvers"].split():
        tag_name = tagset_name.replace("%{EL}", dver)
        tag_section_name = f"tag {tag_name}"
        try:
            config.add_section(tag_section_name)
            _log.debug(
                "Created section [%s] from [%s]", tag_section_name, tagset_section_name
            )
        except configparser.DuplicateSectionError:
            pass
        for key, value in tagset_section.items():
            if key == "dvers":
                continue
            # Do not overwrite existing options
            if key in config[tag_section_name]:
                continue
            new_value = value.replace("%{EL}", dver)
            _log.debug("Setting {%s:%s} to %r", tag_section_name, key, new_value)
            config[tag_section_name][key] = new_value


def _get_taglist_from_config(config: ConfigParser, tagnames: t.List[str]) -> t.List[Tag]:
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
        if tagnames and tag_name not in tagnames:
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


def parse_config(args: Namespace, config: ConfigParser) -> Distrepos:
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
    return Distrepos(
        dest_root=dest_root,
        working_root=working_root,
        previous_root=previous_root,
        condor_rsync=options_section.get("condor_rsync", DEFAULT_CONDOR_RSYNC),
        koji_rsync=options_section.get("koji_rsync", DEFAULT_KOJI_RSYNC),
        taglist=taglist,
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
    dr = parse_config(args, config)

    _log.info("Program started")
    dr.check_rsync()
    _log.info("rsync check successful. Starting run for %d tags", len(dr.taglist))
    successful, failed = dr.run()
    _log.info("Run completed")

    # Report on the results
    successful_names = [it.name for it in successful]
    failed_names = [it.name for it in failed]
    if successful:
        _log.info("%d tags succeeded: %r", len(successful_names), successful_names)
    if failed:
        _log.error("%d tags failed: %r", len(failed_names), failed_names)
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