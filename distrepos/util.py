import fcntl
import fnmatch
import logging
import os
import subprocess as sp
import typing as t

from distrepos.error import ERR_RSYNC, ProgramError

RSYNC_OK = 0
RSYNC_NOT_FOUND = 23

#
# Functions for locking
#


def acquire_lock(
    lock_path: t.Union[str, os.PathLike], make_parents: bool = True
) -> t.Optional[t.IO]:
    """
    Create and return the handle to a lockfile

    Args:
        lock_path: The path to the lockfile to create; the directory must
            already exist.
        make_parents: Whether to make the parent directories of the lockfile

    Returns: A filehandle to be used with release_lock(), or None if we were
        unable to acquire the lock.
    """
    log = logging.getLogger(__name__)
    try:
        if make_parents:
            os.makedirs(os.path.dirname(lock_path), exist_ok=True)
        filehandle = open(lock_path, "w")
        filedescriptor = filehandle.fileno()
        # Get an exclusive lock on the file (LOCK_EX) in non-blocking mode
        # (LOCK_NB), which causes the operation to raise IOError if some other
        # process already has the lock
        try:
            fcntl.flock(filedescriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return filehandle
        except BlockingIOError:
            log.error(
                "%s", "Another run in progress (unable to lock file %s)", lock_path
            )
            return None
    except OSError as err:
        log.error("OSError creating lockfile at %s: %s", lock_path, err)
        log.debug("Traceback follows", exc_info=True)
        return None


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


#
# Wrappers around process handling and logging
#


def log_ml(lvl: int, msg: str, *args, log: t.Optional[logging.Logger] = None, **kwargs):
    """
    Log a potentially multi-line message by splitting the lines and doing
    individual calls to log.log().  exc_info and stack_info will only be
    printed for the last line.
    """
    if not log:
        log = logging.getLogger(__name__)
    if lvl >= log.getEffectiveLevel():
        orig_kwargs = kwargs.copy()
        msg_lines = (msg % args).splitlines()
        last_line = msg_lines[-1]
        kwargs.pop("exc_info", None)
        kwargs.pop("stack_info", None)
        for line in msg_lines[:-1]:
            log.log(lvl, "%s", line, **kwargs)
        return log.log(lvl, "%s", last_line, **orig_kwargs)


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
    log: t.Optional[logging.Logger] = None,
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
        log: A Logger instance to use for logging
    """
    if not log:
        log = logging.getLogger(__name__)
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
    log.log(
        level,
        "%s %s with exit code %d\n%s",
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
    log: t.Optional[logging.Logger] = None,
    **kwargs,
) -> t.Tuple[bool, sp.CompletedProcess]:
    """
    Helper function to run a command and log its output.  Returns a boolean of
    whether the exit code was acceptable, and the CompletedProcess object.

    See Also: log_proc()
    """
    if isinstance(ok_exit, int):
        ok_exit = [ok_exit]
    if not log:
        log = logging.getLogger(__name__)
    kwargs.setdefault("stdout", sp.PIPE)
    kwargs.setdefault("stderr", sp.PIPE)
    kwargs.setdefault("encoding", "latin-1")

    log.debug("running %r %r", args, kwargs)

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
        log=log,
    )
    return ok, proc


#
# Wrappers around rsync
#


def rsync(
    *args, log: t.Optional[logging.Logger] = None, **kwargs
) -> t.Tuple[bool, sp.CompletedProcess]:
    """
    A wrapper around `subprocess.run` that runs rsync, capturing the output
    and error, printing the command to be run if we're in debug mode.
    Returns an (ok, CompletedProcess) tuple where ok is True if the return code is 0.
    """
    if not log:
        log = logging.getLogger(__name__)
    kwargs.setdefault("stdout", sp.PIPE)
    kwargs.setdefault("stderr", sp.PIPE)
    kwargs.setdefault("encoding", "latin-1")
    cmd = ["rsync"] + [str(x) for x in args]
    log.debug("running %r %r", cmd, kwargs)
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
    log: t.Optional[logging.Logger] = None,
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
    return rsync(*args, log=log)


def log_rsync(
    proc: sp.CompletedProcess,
    description: str = "rsync",
    success_level=logging.DEBUG,
    failure_level=logging.ERROR,
    not_found_is_ok=False,
    log: t.Optional[logging.Logger] = None,
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
        log=log,
    )


#
# Misc
#


def match_globlist(text: str, globlist: t.List[str]) -> bool:
    """
    Return True if `text` matches one of the globs in globlist.
    """
    return any(fnmatch.fnmatch(text, g) for g in globlist)


def check_rsync(koji_rsync: str, log: t.Optional[logging.Logger] = None) -> None:
    """
    Run an rsync listing of the rsync root. If this fails, there is no point
    in proceeding further.
    """
    if not log:
        log = logging.getLogger(__name__)
    description = f"koji-hub rsync endpoint {koji_rsync} directory listing"
    try:
        ok, proc = rsync("--list-only", koji_rsync, timeout=180, log=log)
    except sp.TimeoutExpired:
        log.critical(f"{description} timed out")
        raise ProgramError(
            ERR_RSYNC, "rsync dir listing from koji-hub timed out, cannot continue"
        )
    log_rsync(
        proc,
        description,
        failure_level=logging.CRITICAL,
        log=log,
    )
    if not ok:
        raise ProgramError(
            ERR_RSYNC, "rsync dir listing from koji-hub failed, cannot continue"
        )
