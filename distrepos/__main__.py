#!/usr/bin/env python3
"""
This script rsyncs repos created with "koji dist-repo" and combines them with
other external repos (such as the htcondor repo), then updates the repo
definition files.  The list of repositories is pulled from distrepos.conf;
see the comments in that file for the format.

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

import logging
import sys
import typing as t
from configparser import ConfigParser  # TODO We shouldn't need these
from configparser import ExtendedInterpolation

from distrepos.error import ERR_EMPTY, ERR_FAILURES, ProgramError
from distrepos.params import (
    Options,
    Tag,
    ActionType,
    ReleaseSeries,
    format_tag,
    format_mirror,
    get_args,
    parse_config,
)
from distrepos.tag_run import run_one_tag
from distrepos.mirror_run import update_mirrors_for_tag
from distrepos.symlink_utils import link_static_data, link_latest_release
from distrepos.util import lock_context, check_rsync, log_ml, run_with_log
from distrepos.tarball_sync import update_tarball_dirs

from datetime import datetime
from pathlib import Path

_log = logging.getLogger(__name__)


#
# Functions for dealing with the mirror list
#

def create_mirrorlists(options: Options, tags: t.Sequence[Tag]) -> int:
    """
    Create the files used for mirror lists

    Args:
        options: The global options for the run
        tags: The list of tags to create mirror lists for

    Returns:
        0 if all mirror creations were successful, nonzero otherwise
    """
    successful: t.List[Tag] = []
    failed: t.List[t.Tuple[Tag, str]] = []
    with lock_context(options.lock_dir, 'mirrors', log=_log) as lock_fh:
        if not lock_fh:
            return ERR_FAILURES
        # Generate mirrors for each tag defined in the config file.  Tags are run in series.
        # Keep track of successes and failures.
        try:
            for tag in tags:
                ok, err = update_mirrors_for_tag(options, tag)
                if ok:
                    _log.info(f"Mirrors generated for tag {tag.name}")
                    successful.append(tag)
                else:
                    _log.error(f"Mirrors failed to generate for for tag {tag.name}: {err}")
                    failed.append((tag, err))
        except Exception as e:
            _log.error(f"Unexpected error while processing mirrors: {e}")
            return ERR_FAILURES

    # Report on the results
    successful_names = [it.name for it in successful]
    if successful:
        log_ml(
            logging.INFO,
            "%d tag mirror lists generated successfully:\n  %s",
            len(successful_names),
            "\n  ".join(successful_names),
        )
    if failed:
        _log.error("%d tag mirror lists failed:", len(failed))
        for tag, err in failed:
            _log.error("  %-40s: %s", tag.name, err)
        return ERR_FAILURES
    elif not successful:
        _log.error("No tags mirror lists were generated")
        return ERR_EMPTY

    return 0


def rsync_repos(options: Options, tags: t.Sequence[Tag]) -> int:
    """
    Sync repo directories from their build source on Koji to the repo host

    Args:
        options: The global options for the run
        tags: The list of tags to rsync from Koji

    Returns:
        0 if all rsyncs were successful, nonzero otherwise
    """
    # First check that koji hub is even reachable.  If not, there is no point
    # in proceeding further.
    _log.info("Program started")
    check_rsync(options.koji_rsync)
    _log.info("rsync check successful. Starting run for %d tags", len(tags))
    total_start_time = datetime.now()

    # Run each tag defined in the config file.  Tags are run in series.
    # Keep track of successes and failures.
    successful = []
    failed = []
    for tag in tags:
        _log.info("----------------------------------------")
        _log.info("Starting tag %s", tag.name)
        log_ml(
            logging.DEBUG,
            "%s",
            format_tag(
                tag,
                koji_rsync=options.koji_rsync,
                condor_rsync=options.condor_rsync,
                destroot=options.dest_root,
            ),
        )
        tag_start_time = datetime.now()
        ok, err = run_one_tag(options, tag)
        tag_elapsed_time = datetime.now() - tag_start_time
        if ok:
            _log.info("Tag %s completed in %s", tag.name, tag_elapsed_time)
            successful.append(tag)
        else:
            _log.error("Tag %s failed in %s", tag.name, tag_elapsed_time)
            failed.append((tag, err))

    total_elapsed_time = datetime.now() - total_start_time
    _log.info("----------------------------------------")
    _log.info("Run completed in %s", total_elapsed_time)

    # Report on the results
    successful_names = [it.name for it in successful]
    if successful:
        log_ml(
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

def update_cadist(options: Options) -> int:
    """
    Run repo-update-cadist (from https://github.com/opensciencegrid/repo-update-cadist/).
    Pass along its return code

    TODO repo-update-cadist redirects its own stdout/err to log files which doesn't work well
         with subprocess' output piping. For now, just log basic success/failure info
    """
    _log.info("Starting repo-update-cadist")

    with lock_context(options.lock_dir, 'cadist', log=_log) as lock_fh:
        if not lock_fh:
            return ERR_FAILURES
        ok, cadist_proc = run_with_log('/usr/bin/repo-update-cadist', log=_log)
        if ok:
            _log.info("repo-update-cadist succeeded")
        else :
            _log.warning(f"repo-update-cadist failed with status code {cadist_proc.returncode}")
        return cadist_proc.returncode


def update_repo_timestamp(options: Options):
    """
    At the completion of a successful repo sync, update the time listed in the top-level
    timestamp.txt file
    """
    timestamp_txt = Path(options.dest_root) / 'osg' / 'timestamp.txt'
    timestamp_txt.parent.mkdir(parents=True, exist_ok=True)
    with open(timestamp_txt, 'w') as f:
        f.write(datetime.now().strftime("%a %d %b %Y %H:%M:%S %Z"))


def link_static(options: Options) -> int:
    """
    Create symlinks from the main data directory to the static data directory
    """

    _log.info("Updating symlinks to static-data directory")
    try:
        ok, err = link_static_data(options)
        if ok:
            _log.info("static-data symlinks updated successfully")
            return 0
        else:
            _log.warning(f"Unable to update static-data symlinks: {err}")
            return ERR_FAILURES
    except Exception as e:
        _log.exception(f"Unexpected error updating static-data symlinks: {e}")
        return ERR_FAILURES


def link_release(options: Options, release_series: t.List[ReleaseSeries]) -> int:
    """
    Create symlinks for the latest "osg-release" rpm in the "release" repo of a release series
    """
    _log.info("Updating symlinks to latest osg-release rpm")
    try:
        ok, err = link_latest_release(options, release_series)
        if ok:
            _log.info("latest-release symlinks updated successfully")
            return 0
        else:
            _log.warning(f"Unable to update latest-release symlinks: {err}")
            return ERR_FAILURES
    except Exception as e:
        _log.exception(f"Unexpected error updating latest-release symlinks: {e}")
        return ERR_FAILURES


def sync_tarballs(options: Options) -> int:
    """
    Sync client tarballs from an upstream rsync server to repo
    """
    _log.info("Syncing tarball cients")
    try:
        ok, err = update_tarball_dirs(options)
        if ok:
            _log.info("tarball clients updated successfully")
            return 0
        else:
            _log.warning(f"Unable to sync tarball clietns: {err}")
            return ERR_FAILURES
    except Exception as e:
        _log.exception(f"Unexpected error syncing tarball clients: {e}")
        return ERR_FAILURES

#
# Main function
#
def main(argv: t.Optional[t.List[str]] = None) -> int:
    """
    Main function.   Call the functions to parse arguments and config,
    and set up logging and the parameters for each run.
    If --print-tags is specified, only print the tag definitions that were
    parsed from the config file; otherwise, do the run.

    Return the exit code of the program.  Success (0) is if at least one tag succeeded
    and no tags failed.
    """
    args = get_args(argv or sys.argv)
    config_path: str = args.config
    config = ConfigParser(interpolation=ExtendedInterpolation())
    config.read(config_path)

    options, series, taglist = parse_config(args, config)

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
    if args.print_mirrors:
        for tag in taglist:
            print(
                format_mirror(
                    tag,
                    mirror_root=options.mirror_root,
                    mirror_hosts=options.mirror_hosts,
                )
            )
            print("------")
    if args.print_tags or args.print_mirrors:
        return 0

    result = 0
    if ActionType.RSYNC in args.action:
        result = rsync_repos(options, taglist)

    if ActionType.CADIST in args.action and not result:
        result = update_cadist(options)

    if ActionType.MIRROR in args.action and not result:
        result = create_mirrorlists(options, taglist)

    if ActionType.LINK_STATIC in args.action and not result:
        result = link_static(options)

    if ActionType.TARBALL_SYNC in args.action and not result:
        result = sync_tarballs(options)

    if ActionType.LINK_RELEASE in args.action and not result:
        result = link_release(options, series)

    # If all actions were successful, update the repo timestamp
    if not result:
        update_repo_timestamp(options)

    return result


if __name__ == "__main__":
    try:
        sys.exit(main())
    except ProgramError as e:
        _log.error("%s", e)
        _log.debug("Traceback follows", exc_info=True)
        sys.exit(e.returncode)
