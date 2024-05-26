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


DEFAULT_CONDOR_RSYNC = "rsync://rsync.cs.wisc.edu/htcondor"
DEFAULT_KOJI_RSYNC = "rsync://kojihub2000.chtc.wisc.edu::repos-dist"
DEFAULT_DESTROOT = "/var/www/repo"


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
    condor_arch_repos: t.List[SrcDst]
    condor_source_repos: t.List[SrcDst]
    arches: t.List[str]


#
# Other functions
#


def rsync(*args, **kwargs):
    """
    A wrapper around `subprocess.run` that runs rsync, capturing the output
    and error, printing the command to be run if we're in debug mode.
    """
    kwargs.setdefault("stdout", sp.PIPE)
    kwargs.setdefault("stderr", sp.PIPE)
    kwargs.setdefault("encoding", "latin-1")
    cmd = ["rsync"] + [str(x) for x in args]
    if _debug:
        _log.debug("running %r %r", cmd, kwargs)
    return sp.run(cmd, **kwargs)


#
# Main Distrepos class
#


class Distrepos:
    """
    The Distrepos class contains the parameters and code for one run of repo updates
    of all tags in the config.
    """

    def __init__(
        self,
        destroot: t.Union[os.PathLike, str],
        koji_rsync: str,
        condor_rsync: str,
        taglist: t.List[Tag],
    ):
        self.taglist = taglist
        self.destroot = Path(destroot)
        self.newroot = self.destroot / ".new"
        self.oldroot = self.destroot / ".old"
        self.koji_rsync = koji_rsync
        self.condor_rsync = condor_rsync

    def check_rsync(self):
        """
        Run an rsync listing of the rsync root. If this fails, there is no point
        in proceeding further.
        """
        try:
            ret = rsync("--list-only", self.koji_rsync, check=True)
            _log.debug(
                "koji-hub rsync endpoint directory listing succeeded\n"
                "Stdout:\n%s\n"
                "Stderr:\n%s",
                ret.stdout,
                ret.stderr,
            )
        except OSError as err:
            raise ProgramError(ERR_RSYNC, f"Invoking rsync failed: {err}") from err
        except sp.CalledProcessError as err:
            raise ProgramError(
                ERR_RSYNC,
                f"koji-hub rsync endpoint directory listing failed with exit code {err.returncode}\n"
                f"Stdout:\n{err.stdout}\n"
                f"Stderr:\n{err.stderr}\n",
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
        destpath = self.newroot / tag.dest
        linkpath = self.destroot / tag.dest
        os.makedirs(destpath, exist_ok=True)
        latest_dir = self._get_latest_dir(tag.source)
        if not latest_dir:
            _log.error("Couldn't get the latest dir for tag %s", tag.name)
            return False
        sourcepath = f"{self.koji_rsync}/{tag.source}/{latest_dir}/"
        if not self._rsync_one_tag(sourcepath, destpath, linkpath):
            return False
        if not self._rearrange_rpms(Path(destpath), tag.arches):
            return False
        if not self._merge_condor_repos(
            tag.condor_arch_repos, tag.condor_source_repos, tag.arches
        ):
            return False
        if not self._run_createrepo(destpath, tag.arches):
            return False
        if not self._run_repoview(destpath, tag.arches):
            return False

    def _get_latest_dir(self, tagdir: str) -> t.Optional[str]:
        """
        Resolves the "latest" symlink for the dist-repo on koji-hub by downloading
        the symlink to a temporary directory and reading it.
        """
        with tempfile.TemporaryDirectory as tempdir:
            destpath = os.path.join(tempdir.name, "latest")
            try:
                ret = rsync(
                    "-l", f"{self.koji_rsync}/{tagdir}/latest", destpath, check=True
                )
                _log.debug(
                    "Success in rsync\nStdout:\n%s\nStderr:\n%s", ret.stdout, ret.stderr
                )
            except OSError as err:
                raise ProgramError(ERR_RSYNC, f"Invoking rsync failed: {err}") from err
            except sp.CalledProcessError as err:
                # XXX what does rsync return for file not found?
                _log.error(
                    "Return code %s in rsync\nStdout:\n%s\nStderr:\n%s",
                    err.returncode,
                    err.stdout,
                    err.stderr,
                )
                return None
            # we have copied the "latest" symlink as a (now broken) symlink. Read the text of the link to get
            # the directory on the remote side.
            return os.path.basename(os.readlink(destpath))

    @staticmethod
    def _rsync_with_link(
        sourcepath: str,
        destpath: t.Union[str, os.PathLike],
        linkpath: t.Union[None, str, os.PathLike],
        description: str = "rsync",
    ) -> bool:
        """
        rsync from a remote URL sourcepath to the destination destpath, optionally
        linking to files in linkpath.

        Return True on success, False on failure
        """
        args = [
            "--recursive",
            "--times",
            "--delete",
        ]
        if linkpath and os.path.exists(linkpath):
            args.append(f"--link-path={linkpath}")
        args += [
            sourcepath,
            destpath,
        ]
        try:
            ret = rsync(*args, check=True)
            _log.debug(
                f"{description}: Success.\nStdout:\n%s\nStderr:\n%s",
                ret.stdout,
                ret.stderr,
            )
        except sp.CalledProcessError as err:
            _log.error(
                f"{description}: Return code %s.\nStdout:\n%s\nStderr:\n%s",
                err.returncode,
                err.stdout,
                err.stderr,
            )
            return False
        return True

    def _rsync_one_tag(self, sourcepath, destpath, linkpath) -> bool:
        """
        rsync the distrepo from kojihub for one tag, linking to the RPMs in
        the previous repo if they exist
        """
        # XXX rsync source and binaries separately so the rearranging doesn't
        # screw up the linking
        return self._rsync_with_link(
            sourcepath, destpath, linkpath, "rsync from koji-hub"
        )

    def _rearrange_rpms(self, destpath: Path, arches: t.List[str]) -> bool:
        """
        Rearrange the rsynced RPMs to go from a distrepo layout to something resembling
        the old mash-created repo layout -- in particular, the repodata dirs need to
        be in the same location as in the old repo layout, so we don't have to change
        the .repo files we ship.

        The mash-created repo layout looks like
            (destpath)/source/SRPMS/{*.src.rpm,repodata/,repoview/}
            (destpath)/x86_64/{*.rpm,repodata/,repoview/}
            (destpath)/x86_64/debug/{*-{debuginfo,debugsource}*.rpm,repodata/,repoview/}

        The distrepo layout looks like (where <X> is the first letter of the package name)
            (destpath)/src/repodata/
            (destpath)/src/pkglist
            (destpath)/src/Packages/<X>/*.src.rpm
            (destpath)/x86_64/repodata/
            (destpath)/x86_64/pkglist
            (destpath)/x86_64/debug/pkglist
            (destpath)/x86_64/debug/repodata/
            (destpath)/x86_64/Packages/<X>/{*.rpm, *-{debuginfo,debugsource}*.rpm}

        Note that the debuginfo and debugsource rpm files are mixed in with the regular files.
        The "pkglist" files are text files listing the relative paths to the packages in the
        repo -- this is passed to `createrepo` to put the debuginfo and debugsource RPMs into
        separate repositories even though the files are mixed together.
        """
        # First, SRPMs. No major rearranging needed, just src -> source/SRPMS
        try:
            (destpath / "source").mkdir(parents=True, exist_ok=True)
            if (destpath / "source/SRPMS").exists():
                shutil.rmtree(destpath / "source/SRPMS")
            shutil.move(destpath / "src", destpath / "source/SRPMS")
        except OSError as err:
            _log.error("OSError rearranging SRPMs: %s", err, exc_info=_debug)
            return False

        # Next, binary RPMs.
        for arch in arches:
            try:
                # XXX If we don't care about RPM locations just repodata/repoview locations then binary RPMs are already OK
                pass
            except OSError as err:
                _log.error(
                    "OSError rearranging binary RPMs for arch %s: %s",
                    arch,
                    err,
                    exc_info=_debug,
                )
                return False

        raise True

    def _merge_condor_repos(
        self,
        arch_repos: t.List[SrcDst],
        source_repos: t.List[SrcDst],
        arches: t.List[str],
    ) -> bool:
        for repo in arch_repos:
            for arch in arches:
                src = f"{self.condor_rsync}/{repo.src}/".replace("<ARCH>", arch)
                dst = f"{self.newroot}/{repo.dst}/".replace("<ARCH>", arch)
                link = f"{self.destroot}/{repo.dst}/".replace("<ARCH>", arch)
                ok = self._rsync_with_link(
                    src, dst, link, description=f"rsync from condor repo for {arch}"
                )
                if not ok:
                    return False
        for repo in source_repos:
            src = f"{self.condor_rsync}/{repo.src}/"
            dst = self.newroot / repo.dst
            link = self.destroot / repo.dst
            ok = self._rsync_with_link(
                src, dst, link, description=f"rsync from condor repo for SRPMS"
            )
            if not ok:
                return False
        return True

    def _run_createrepo(self, destpath: Path, arches: t.List[str]) -> bool:
        raise NotImplementedError()

    def _run_repoview(self, destpath: Path, arches: t.List[str]) -> bool:
        raise NotImplementedError()


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
    """Parse and validate command-line arguments"""
    parser = ArgumentParser(prog=argv[0], description=__doc__)
    parser.add_argument(
        "--config",
        default="/etc/distrepos.conf",
        help="Config file to pull tag and repository information from.",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Output debug messages",
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
        "relative to this directory.",
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
    value of the tagset's 'dvers' option into "<EL>".
    Modifies 'config' in-place.
    """
    if "<EL>" not in tagset_section_name:
        raise ConfigError(
            f"Section name [{tagset_section_name}] does not contain '<EL>'"
        )
    tagset_section = config[tagset_section_name]
    tagset_name = tagset_section_name.split(" ", 1)[1].strip()

    # Check for the option we're supposed to be looping over
    if not tagset_section.get("dvers"):
        raise MissingOptionError(tagset_section_name, "dvers")
    # Also check for the options that are supposed to be in the 'tag' sections, otherwise
    # we'd get some confusing error messages when we get to parsing those.
    if not tagset_section.get("dest"):
        raise MissingOptionError(tagset_section_name, "dest")
    if not tagset_section.get("arches"):
        raise MissingOptionError(tagset_section_name, "arches")

    # Loop over the dvers, expand into tag sections
    for dver in tagset_section["dvers"].split():
        tag_name = tagset_name.replace("<EL>", dver)
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
            new_value = value.replace("<EL>", dver)
            _log.debug("Setting {%s:%s} to %r", tag_section_name, key, new_value)
            config[tag_section_name][key] = new_value


def _get_taglist_from_config(config: ConfigParser) -> t.List[Tag]:
    """
    Parse the 'tag' and 'tagset' sections in the config to return a list of Tag objects.
    This calls _expand_tagset to expand tagset sections, which may modify the config object.
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
        if not section.lower().startswith("tag "):
            continue

        tag_name = section_name.split(" ", 1)[1].strip()
        source = section.get("source", tag_name)
        dest = section.get("dest")
        if not dest:
            raise MissingOptionError(section_name, "dest")
        arches = section.get("arches", "").split()
        if not arches:
            raise MissingOptionError(section_name, "arches")
        condor_arch_repos = get_source_dest_opt(section.get("condor_arch_repos", ""))
        condor_source_repos = get_source_dest_opt(
            section.get("condor_source_repos", "")
        )
        taglist.append(
            Tag(
                name=tag_name,
                source=source,
                dest=dest,
                arches=arches,
                condor_arch_repos=condor_arch_repos,
                condor_source_repos=condor_source_repos,
            )
        )

    return taglist


def parse_config(args: Namespace, config: ConfigParser) -> Distrepos:
    """
    Parse the config file and return the Distrepos object from the parameters.
    Apply any overrides from the command-line.
    """
    taglist = _get_taglist_from_config(config)
    if not taglist:
        raise ConfigError("No [tag ...] or [tagset ...] sections found")

    if "options" not in config:
        raise ConfigError("Missing required section [options]")
    options_section = config["options"]
    return Distrepos(
        destroot=args.destroot or options_section.get("destroot", DEFAULT_DESTROOT),
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
    dr.check_rsync()
    successful, failed = dr.run()

    if successful:
        _log.info("The following tags succeeded: %r", [tt.name for tt in successful])
    if failed:
        _log.error("The following tags failed: %r", [tt.name for tt in failed])
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
