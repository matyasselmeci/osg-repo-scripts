#!/usr/bin/env python3
"""
This script rsyncs repos created with "koji dist-repo" and combines them with
other external repos (such as the htcondor repo), then updates the repo
definition files.  The list of repositories is pulled from a config file.
"""

import configparser
import tempfile
from configparser import ConfigParser, ExtendedInterpolation

# import glob
import logging
import logging.handlers
import os

# import pathlib
import re

# import shutil
import subprocess as sp
import sys
import typing as t
from argparse import ArgumentParser, Namespace

# from pathlib import Path


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
            f"Section {section_name} missing required option {option_name}"
        )


def rsync(*args, **kwargs):
    """
    A wrapper around `subprocess.run` that runs rsync, capturing the output
    and error, printing the command to be run if we're in debug mode.
    """
    kwargs.setdefault("stdout", sp.PIPE)
    kwargs.setdefault("stderr", sp.PIPE)
    kwargs.setdefault("encoding", "latin-1")
    cmd = ["rsync"] + list(args)
    if _debug:
        _log.debug("running %r %r", cmd, kwargs)
    return sp.run(cmd, **kwargs)


class SrcDst(t.NamedTuple):
    """A source/destination pair"""

    src: str
    dst: str


class Tag(t.NamedTuple):
    name: str
    sourcedir: str
    dest: str
    condor_arch_repos: t.List[SrcDst]
    condor_source_repos: t.List[SrcDst]
    arches: t.List[str]


class Distrepos:
    def __init__(self, destroot: str, koji_rsync: str, condor_rsync: str, taglist: t.List[Tag]):
        self.taglist = taglist
        self.destroot = destroot
        self.newroot = destroot + ".new"
        self.oldroot = destroot + ".old"
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
        destpath = os.path.join(self.newroot, tag.dest)
        linkpath = os.path.join(self.destroot, tag.dest)
        os.makedirs(destpath, exist_ok=True)
        latest_dir = self._get_latest_dir(tag.sourcedir)
        if not latest_dir:
            _log.error("Couldn't get the latest dir for tag %s", tag.name)
            return False
        sourcepath = f"{self.koji_rsync}/{tag.sourcedir}/{latest_dir}/"
        if not self._rsync_one_tag(sourcepath, destpath, linkpath):
            return False
        if not self._rearrange_rpms(destpath):
            return False
        if not self._merge_condor_repos(tag.condor_arch_repos, tag.condor_source_repos):
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

    def _rsync_one_tag(self, sourcepath, destpath, linkpath) -> bool:
        """
        rsync the distrepo from kojihub for one tag, linking to the RPMs in
        the previous repo if they exist
        """
        # XXX isn't rearranging going to screw up linking?
        args = [
            "--recursive",
            "--times",
            "--delete",
        ]
        if os.path.exists(linkpath):
            args.append(f"--link-path={linkpath}")
        args += [
            sourcepath,
            destpath,
        ]
        try:
            ret = rsync(*args, check=True)
            _log.debug(
                "Success in rsync from koji-hub\nStdout:\n%s\nStderr:\n%s",
                ret.stdout,
                ret.stderr,
            )
        except OSError as err:
            raise ProgramError(ERR_RSYNC, f"Invoking rsync failed: {err}") from err
        except sp.CalledProcessError as err:
            _log.error(
                "Return code %s in rsync from koji-hub\nStdout:\n%s\nStderr:\n%s",
                err.returncode,
                err.stdout,
                err.stderr,
            )
            return False
        return True

    def _rearrange_rpms(self, destpath: str) -> bool:
        # TODO Write code for rearranging
        # The mash-created repo layout looks like
        #   (root)/source/SRPMS/{*.src.rpm,repodata/,repoview/}
        #   (root)/x86_64/{*.rpm,repodata/,repoview/}
        #   (root)/x86_64/debug/{*-{debuginfo,debugsource}*.rpm,repodata/,repoview/}
        #
        # The distrepo layout looks like (where <X> is the first letter of the package name)
        #   (root)/src/repodata/
        #   (root)/src/pkglist
        #   (root)/src/Packages/<X>/*.src.rpm
        #   (root)/x86_64/repodata/
        #   (root)/x86_64/pkglist
        #   (root)/x86_64/debug/pkglist
        #   (root)/x86_64/debug/repodata/
        #   (root)/x86_64/Packages/<X>/{*.rpm, *-{debuginfo,debugsource}*.rpm}
        # Note that the debuginfo and debugsource rpm files are mixed in with the regular files.
        # The "pkglist" files are text files listing the relative paths to the packages in the
        # repo -- this is passed to `createrepo` to put the debuginfo and debugsource RPMs into
        # separate repositories even though the files are mixed together.
        raise NotImplementedError()

    def _merge_condor_repos(
        self, arch_repos: t.List[SrcDst], source_repos: t.List[SrcDst]
    ) -> bool:
        raise NotImplementedError()

    def _run_createrepo(self, destpath: str, arches: t.List[str]) -> bool:
        raise NotImplementedError()

    def _run_repoview(self, destpath: str, arches: t.List[str]) -> bool:
        raise NotImplementedError()


def get_boolean_option(
    name: str, args: Namespace, config: configparser.ConfigParser
) -> bool:
    """
    Gets the value of a boolean config file option, which can also be
    turned on by a command-line argument of the same name.
    Raise ConfigError if the option is not a boolean.
    """
    try:
        cmdarg = getattr(args, name, None)
        if cmdarg is None:
            return config["options"].getboolean(name, fallback=False)
        else:
            return bool(cmdarg)
    except ValueError:
        raise ConfigError(f"'{name}' must be a boolean")


def get_source_dest_opt(option: str) -> t.List[SrcDst]:
    """
    Parse a config option of the form
        SRC1 -> DST1
        SRC2 -> DST2
    Returning a list of SrcDst objects.
    Blank lines are ignored.
    Leading and trailing whitespace are stripped.
    A warning is emitted for invalid lines.
    """
    ret = []
    for line in option.splitlines():
        line = line.strip()
        if not line:
            continue
        mm = re.fullmatch(r"(.+?)\s*->\s*(.+?)", line)
        if mm:
            ret.append(SrcDst(mm.group(1), mm.group(2)))
        else:
            _log.warning("Skipping invalid source->dest line %r", line)
    return ret


def get_args(argv: t.List[str]) -> Namespace:
    """Parse and validate command-line arguments"""
    parser = ArgumentParser(prog=argv[0], description=__doc__)
    parser.add_argument(
        "--config",
        default="/etc/pullrepos.conf",
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


def parse_config(args: Namespace, config: ConfigParser) -> Distrepos:
    """
    Parse the config file and return the Distrepos object from the parameters.
    Apply any overrides from the command-line.
    """
    if "options" not in config:
        raise ConfigError("Missing required section 'options'")
    options_section = config["options"]
    taglist = []
    for section_name, section in config.items():
        if not section.lower().startswith("tag "):
            continue
        tag = section.split(" ", 1)[1].strip()
        sourcedir = section.get("sourcedir", tag)
        if "dest" not in section:
            raise MissingOptionError(section_name, "dest")
        if "arches" not in section:
            raise MissingOptionError(section_name, "arches")
        condor_arch_repos = get_source_dest_opt(section.get("condor_arch_repos"))
        condor_source_repos = get_source_dest_opt(section.get("condor_source_repos"))
        taglist.append(
            Tag(
                name=tag,
                sourcedir=sourcedir,
                dest=section["dest"],
                arches=section["arches"].split(),
                condor_arch_repos=condor_arch_repos,
                condor_source_repos=condor_source_repos,
            )
        )
    if not taglist:
        raise ConfigError("No 'tag' sections found")
    return Distrepos(
        destroot=args.destroot or options_section.get("destroot", DEFAULT_DESTROOT),
        condor_rsync=options_section.get("condor_rsync", DEFAULT_CONDOR_RSYNC),
        koji_rsync=options_section.get("koji_rsync", DEFAULT_KOJI_RSYNC),
        taglist=taglist,
    )


def main(argv: t.Optional[t.List[str]] = None) -> int:
    global _debug

    args = get_args(argv or sys.argv)
    config_path: str = args.config
    config = ConfigParser(interpolation=ExtendedInterpolation())
    config.read(config_path)

    _debug = get_boolean_option("debug", args, config)
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
