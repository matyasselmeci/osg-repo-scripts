"""
This module contains the functions for populating mirrors for a single tag.
The main entry point is update_mirrors_for_tag(); other functions are helpers.
"""

import logging
from distrepos.params import Options, Tag
from distrepos.tag_run import update_release_repos
import typing as t
import socket
import string
import os
import requests
from datetime import datetime, timedelta
from pathlib import Path

_log = logging.getLogger(__name__)


def get_baseline_urls() -> t.List[str]:
    """
    Get the
    """
    timeout = 5
    socket.setdefaulttimeout(timeout)
    fqdn = socket.getfqdn()
    if "osgdev" in fqdn or "osg-dev" in fqdn:
        return ["https://repo-itb.osg-htc.org"]
    else:
        return ["https://repo.osg-htc.org"]


def get_mirror_info_for_arch(hostname: str, tag: Tag, arch: str) -> t.Tuple[str, str]:
    """
    Given the top level domain of a potential mirror, find the expected path for that domain
    that would contain a mirror of the given tag.
    """
    path_arch = string.Template(tag.arch_rpms_mirror_base).safe_substitute(
        {"ARCH": arch}
    )
    # TODO this might be a misuse of os.path.join. The more appropriate function,
    # urllib.parse.urljoin, is very sensitive to leading/trailing slashes in the path parts though
    mirror_base = os.path.join(hostname, path_arch)
    repomd_url = os.path.join(mirror_base, 'repodata/repomd.xml')
    return mirror_base, repomd_url


def test_single_mirror(repodata_url: str) -> bool:
    """
    Given the full URL of a repodata/repomd.xml that might mirror a tag, return whether
    that file exists and was updated in the past 24 hours.
    """
    _log.info(f"Checking for existence and up-to-dateness of {repodata_url}")
    response = requests.get(repodata_url, timeout=10)
    if response.status_code != 200:
        _log.warning(
            f"bad(non 200) response.code for mirror {repodata_url}: {response.status_code}"
        )
        return False
    else:
        # make sure the repository is up-to-date
        lastmod_str = response.headers.get("Last-Modified")
        if not lastmod_str:
            _log.warning(
                f"Mirror {repodata_url} missing expected 'Last-Modified' header"
            )
            return False
        lastmodtime = datetime.strptime(
            lastmod_str, "%a, %d %b %Y %H:%M:%S %Z"
        )  # Sun, 15 Sep 2024 13:34:06 GMT
        age = datetime.now() - lastmodtime
        if datetime.now() - lastmodtime > timedelta(hours=24):
            _log.warning(
                f"Mirror {repodata_url} too old ({age} seconds old) Last-Modified: {lastmod_str} ... ignoring"
            )
            return False
        else:
            _log.debug(f"Mirror {repodata_url} all good")
            return True


def update_mirrors_for_tag(options: Options, tag: Tag) -> t.Tuple[bool, str]:
    """
    For a given tag, check whether every known mirror host contains an up-to-date mirror
    of that tag's repo. Update the mirrorlist file for that tag.

    Args:
        options: The global options for the run
        tag: The specific tag to check mirrors for

    Returns:
        An (ok, error message) tuple.
    """

    mirror_hostnames = get_baseline_urls() + options.mirror_hosts

    working_path = Path(options.mirror_working_root) / tag.dest
    prev_path = Path(options.mirror_prev_root) / tag.dest
    dest_path = Path(options.mirror_root) / tag.dest

    for arch in tag.arches:
        good_mirrors = []
        for hostname in mirror_hostnames:
            _log.info(f"Checking mirror {hostname}")
            mirror_base, repodata_url = get_mirror_info_for_arch(hostname, tag, arch)
            if test_single_mirror(repodata_url):
                good_mirrors.append(mirror_base)

        # TODO is it a failure if no mirrors are found outside of osg-hosted repos? Assume no
        if not good_mirrors:
            return False, f"No good mirrors found for tag {tag.name}, arch {arch}"


        _log.info(f"Writing working mirror file {working_path}")
        # ensure the output path exists
        working_path.mkdir(parents=True, exist_ok=True)

        with open(working_path / arch, 'w') as mirrorf:
            mirrorf.write('\n'.join(good_mirrors))

    update_release_repos(dest_path, working_path, prev_path)

    return True, ""
