#!/usr/bin/env python3
"""
Migration script from mosh-based repo layout to distrepos-based repo layout.
This script rearranges the repo directories given on the command line
to the layout used by the EL9 repo server, which, among a few other changes,
groups RPMs into subdirectories based on their starting letter.

To work with existing repo metadata, the script creates symlinks from the new
locations to the old locations.

A migration should be followed up with an rsync from the new repo server,
with the `--delay-updates --delete-delay` flags added.  For example,

    rsync -av --delay-updates --delete-delay \\
        repo-rsync-itb.osg-htc.org::osg/24-main/ /mirror/osg/24-main

This will update the metadata and delete the symlinks, making sure everything
has been downloaded before making any changes.
"""

import logging
import os
import re
import shutil
import sys
import typing as t
from argparse import ArgumentParser, RawDescriptionHelpFormatter
from pathlib import Path

BINARY_ARCHES = ["aarch64", "x86_64"]
CONDOR_RPM_GLOBS = [
    "condor-*.rpm",
    "htcondor-ce-*.rpm",
    "htcondor-release-*.rpm",
    "minicondor-*.rpm",
    "pelican-*.rpm",
    "python3-condor-*.rpm",
]


_log = logging.getLogger(__name__)


def move_and_symlink(frompath: os.PathLike, topath: os.PathLike):
    """
    Move a file and create a symlink at its original location pointing
    to its new location.
    """
    os.rename(frompath, topath)
    os.symlink(os.path.relpath(topath, os.path.dirname(frompath)), frompath)


def hardlink_or_copy_file(frompath: os.PathLike, topath: os.PathLike):
    """
    Try to hardlink a file from one path to another; if that fails,
    make a copy instead.
    """
    try:
        os.link(frompath, topath)
    except OSError:
        shutil.copy2(frompath, topath)


def get_condor_package_subdirs(repo: Path):
    """
    Get the names of the Packages/condor-* subdirectories for the given
    repo based on if it's development, release, or testing.
    If we don't know, return all three possibilities.
    """
    if repo.name == "debug" or repo.name == "SRPMS":
        parent_name = repo.resolve().parent.parent.name
    else:
        parent_name = repo.resolve().parent.name
    if parent_name in ["testing", "release"]:
        return [
            "condor-release",
            "condor-update",
        ]
    elif parent_name == "development":
        return ["condor-daily"]
    else:
        return [
            "condor-release",
            "condor-update",
            "condor-daily",
        ]


def migrate_one_repo(repo: Path, packages_dir: Path, dry_run: bool = False) -> bool:
    """
    Migrate all of the RPMs in one repo to the new layout.  Skips a repo if
    there are any RPMs from OSG 3.6 or earlier, since the layouts for those
    repos didn't change.

    Args:
        repo: The repo directory to migrate.
        packages_dir: The Packages directory to move RPMs to.  Symlinks will be
            created in the original locations.
        dry_run: Set this to True to avoid making actual changes and only print
            what would be done.

    Returns:
        True if RPMs were migrated, False if the migration was skipped, for
        example due to pre-OSG-23 RPMs being found.
    """
    all_rpms = sorted(repo.glob("*.rpm"))
    for rpm in all_rpms:
        if re.search(r"[.]osg(3[123456]|devops)", rpm.name):
            _log.warning(f"Pre-OSG-23 RPM found: {rpm}.  Not migrating {repo}")
            return False

    condor_package_subdirs = get_condor_package_subdirs(repo)

    for rpm in all_rpms:
        if rpm.is_symlink():
            # This directory might have already been migrated.
            _log.debug(f"Skipping symlink {rpm}")
            continue

        # The new repo layout puts RPMs taken from the Condor repos into
        # subdirectories based on which Condor repo they were taken from.
        is_condor_rpm = any(rpm.match(gl) for gl in CONDOR_RPM_GLOBS)
        if is_condor_rpm:
            destdir = packages_dir / condor_package_subdirs[0]
        # Other RPMs are moved into directories based on the first letter of
        # the RPM (or '0' if the first character is a number).
        elif rpm.name[0] in "0123456789":
            destdir = packages_dir / "0"
        else:
            destdir = packages_dir / rpm.name[0].lower()

        destfile = destdir / rpm.name
        _log.info(f"Move {rpm} to {destfile}")
        if not dry_run:
            destdir.mkdir(exist_ok=True, parents=True)
            move_and_symlink(rpm, destfile)

        if is_condor_rpm:
            # The Condor RPMs in this repo might be from a combination of UW
            # repos, e.g., both condor-release and condor-update.  We don't
            # know _which_ condor repo they were taken from so to be safe,
            # put the RPM in all of them.  Use hardlinks if possible to save
            # disk space.
            for other_subdir in condor_package_subdirs[1:]:
                other_destdir = packages_dir / other_subdir
                other_destfile = other_destdir / rpm.name
                _log.info(f"Copy {rpm} to {other_destfile}")
                if not dry_run:
                    other_destdir.mkdir(exist_ok=True, parents=True)
                    hardlink_or_copy_file(rpm, other_destfile)

    return True


def migrate_source(args):
    """
    Migrate SRPMs.  This is two steps:
    1. Move the RPMs into Packages/<letter> subdirectories as usual
    2. Move the `source/SRPMS` dir to `src` and create a compat symlink.

    If step 1 does not migrate any RPMs (because it's a pre-OSG-23 repo) then
    the rest is skipped.

    If `source/SRPMS` is already a symlink, we assume it's been migrated
    and leave it alone.  Also if `src` exists, we assume it's been migrated
    and also do nothing.
    """
    for repo in repos(args.dirs):
        if repo.parts[-2:] != ("source", "SRPMS"):
            continue
        if repo.is_symlink():
            _log.info(f"{repo} is already a symlink; skipping")
            return
        dest = repo.resolve().parent.parent / "src"
        if dest.exists():
            _log.info(f"{dest} already exists; skipping")
            return

        _log.info(f"Migrating {repo}")
        if migrate_one_repo(repo, repo / "Packages", dry_run=args.dry_run):
            _log.info(f"Rename {repo} to {dest} and create symlink")
            if not args.dry_run:
                move_and_symlink(repo, dest)
        else:
            _log.info(f"Skipping rename of {repo} to {dest}")


def migrate_binary(args):
    """
    Migrate RPMs in arch-specific repos.
    """
    for repo in repos(args.dirs):
        if repo.name not in BINARY_ARCHES:
            continue
        _log.info(f"Migrating {repo}")
        migrate_one_repo(repo, repo / "Packages", dry_run=args.dry_run)


def migrate_debug(args):
    """
    Migrate the debuginfo and debugsource RPMs.
    In the new repo layout, the debug RPMs are mixed in with the non-debug RPMs,
    though the repo metadata remains in the "debug" subdirectory.  A "pkglist"
    file is used to list which files are in the debug repo vs the main repo,
    but the migrate script uses symlinks instead.
    """
    for repo in repos(args.dirs):
        if repo.name != "debug" and repo.parent.name not in BINARY_ARCHES:
            continue
        _log.info(f"Migrating {repo}")
        migrate_one_repo(repo, repo.parent / "Packages", dry_run=args.dry_run)


def repos(dirs: t.Sequence[os.PathLike]) -> t.Iterator[Path]:
    """
    Iterate over the repos in the directory trees of `dirs`.
    """
    for dir_ in dirs:
        repodatas = Path(dir_).glob("**/repodata")
        for repodata in repodatas:
            repo = repodata.parent
            yield repo


def get_args(argv):
    """
    Parse and validate arguments
    """
    all_actions = ["source", "binary", "debug"]
    parser = ArgumentParser(
        description=__doc__, formatter_class=RawDescriptionHelpFormatter
    )
    parser.add_argument("dirs", nargs="*", help="Directories to migrate")
    parser.add_argument(
        "--source",
        action="append_const",
        dest="actions",
        const="source",
        help="Migrate source RPMs",
    )
    parser.add_argument(
        "--binary",
        action="append_const",
        dest="actions",
        const="binary",
        help="Migrate binary RPMs",
    )
    parser.add_argument(
        "--debug",
        action="append_const",
        dest="actions",
        const="debug",
        help="Migrate debuginfo and debugsource RPMs",
    )
    parser.add_argument(
        "--all",
        action="store_const",
        dest="actions",
        const=all_actions,
        help="Run all migrations (default)",
    )
    parser.add_argument(
        "-n",
        "--dry-run",
        action="store_true",
        help="Only show what would be done, do not migrate",
    )
    parser.set_defaults(actions=[], dirs=[])

    args = parser.parse_args(argv[1:])
    if not args.actions:
        args.actions = all_actions
    return args


def main(argv=None):
    """
    Main function. Get arguments and run the desired actions.
    """
    args = get_args(argv or sys.argv)
    if "source" in args.actions:
        migrate_source(args)
    if "binary" in args.actions:
        migrate_binary(args)
    if "debug" in args.actions:
        migrate_debug(args)

    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG, format="%(message)s")
    sys.exit(main())
