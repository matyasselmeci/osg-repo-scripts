"""
Reads the config file and command-line arguments; sets up logging, the
global Options, and the parameters for each Tag.
"""

import configparser
import logging
import logging.handlers
import os
import re
import string
import sys
import typing as t
from argparse import ArgumentParser, Namespace, RawDescriptionHelpFormatter
from configparser import ConfigParser
from pathlib import Path

from distrepos.error import ConfigError, MissingOptionError
from distrepos.util import match_globlist

MB = 1 << 20
LOG_MAX_SIZE = 500 * MB

DEFAULT_CONDOR_RSYNC = "rsync://rsync.cs.wisc.edu/htcondor"
DEFAULT_CONFIG = "/etc/distrepos.conf"
DEFAULT_DESTROOT = "/usr/local/repo"
DEFAULT_KOJI_RSYNC = "rsync://kojihub2000.chtc.wisc.edu/repos-dist"
DEFAULT_LOCK_DIR = "/var/lock/rsync_dist_repo"

# These options are required to be present _and_ nonempty.  Some of them may
# come from the DEFAULT section.

REQUIRED_TAG_OPTIONS = ["dest", "arches", "arch_rpms_subdir", "source_rpms_subdir"]

_log = logging.getLogger(__name__)


class SrcDst(t.NamedTuple):
    """
    A source/destination pair, used for the definitions of external repos.
    """

    src: str
    dst: str

    def __str__(self):
        return f"{self.src} -> {self.dst}"


class Tag(t.NamedTuple):
    """
    Parameters for a single tag run based on the [tag] and [tagset] sections
    of the config file.
    """

    name: str
    source: str
    dest: str
    arches: t.List[str]
    condor_repos: t.List[SrcDst]
    arch_rpms_dest: str
    debug_rpms_dest: str
    source_rpms_dest: str


class Options(t.NamedTuple):
    """
    Global options that apply to all tag runs.
    """

    dest_root: Path
    working_root: Path
    previous_root: Path
    koji_rsync: str
    condor_rsync: str
    lock_dir: t.Optional[Path]
    mirror_root: t.Optional[Path]
    mirror_hosts: t.List[str]


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


def setup_logging(logfile: t.Optional[str], debug: bool) -> None:
    """
    Sets up logging, given an optional logfile.

    Logs are written to a logfile if one is defined. In addition,
    log to stderr if it's a tty.
    """
    loglevel = logging.DEBUG if debug else logging.INFO
    rootlog = logging.getLogger()
    rootlog.setLevel(loglevel)
    if sys.stderr.isatty():
        ch = logging.StreamHandler()
        ch.setLevel(loglevel)
        chformatter = logging.Formatter(">>>\t%(message)s")
        ch.setFormatter(chformatter)
        rootlog.addHandler(ch)
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
        rootlog.addHandler(rfh)


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


def get_taglist(args: Namespace, config: ConfigParser) -> t.List[Tag]:
    """
    Parse the 'tag' and 'tagset' sections in the config to return a list of Tag objects.
    This calls _expand_tagset to expand tagset sections, which may modify the config object.
    If 'args.tags' is nonempty, limits the tags to only those named in args.tags.

    Args:
        args: command-line arguments parsed by argparse
        config: ConfigParser configuration

    Returns:
        a list of Tag objects
    """
    tagnames = args.tags
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
    Parse the config file and return the tag list and Options object from the parameters.
    Apply any overrides from the command-line.
    Also set up logging.
    """
    if args.debug:
        debug = True
    else:
        try:
            debug = config.getboolean("options", "debug")
        except configparser.Error:
            debug = False

    if args.logfile:
        logfile = args.logfile
    else:
        logfile = config.get("options", "logfile", fallback="")
    setup_logging(logfile, debug)

    taglist = get_taglist(args, config)
    if not taglist:
        raise ConfigError("No (matching) [tag ...] or [tagset ...] sections found")

    options = get_options(args, config)
    return (
        options,
        taglist,
    )


def get_options(args: Namespace, config: ConfigParser) -> Options:
    """
    Build an Options object from the config and command-line arguments.

    Args:
        args: command-line arguments parsed by argparse
        config: ConfigParser configuration

    Returns:
        an Options object
    """
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
    options = Options(
        dest_root=Path(dest_root),
        working_root=Path(working_root),
        previous_root=Path(previous_root),
        condor_rsync=options_section.get("condor_rsync", DEFAULT_CONDOR_RSYNC),
        koji_rsync=options_section.get("koji_rsync", DEFAULT_KOJI_RSYNC),
        lock_dir=Path(args.lock_dir) if args.lock_dir else None,
        mirror_root=mirror_root,
        mirror_hosts=mirror_hosts,
    )
    return options


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
