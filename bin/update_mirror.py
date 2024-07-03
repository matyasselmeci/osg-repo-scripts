#!/usr/bin/python


import urllib.request, urllib.error, urllib.parse
import time
import sys
import os
import shutil
import socket
import fcntl
import errno
import re

def log(line):
    print("[%s]"%sys.argv[0], time.strftime("%a %m/%d/%y %H:%M:%S %Z: ", time.localtime()), line)

def lock(path):
    lockdir = os.path.dirname(path)
    if lockdir and not os.path.exists(lockdir):
        os.makedirs(lockdir)
    lock_fd = os.open(path, os.O_WRONLY | os.O_CREAT)
    try:
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except IOError as e:
        if e.errno == errno.EWOULDBLOCK:
            log("Script appears to already be running.")
            sys.exit(1)

lock("/var/lock/repo/update-mirror.lk")

tagfile = open("/etc/osg-koji-tags/osg-tags", "r")
tags = [tag.rstrip("\n").split(":")[0] for tag in tagfile]
tags = sorted(set(tags))
tagfile.close()

archs = ["x86_64"]
mirrorhosts = [
    # list of mirror base urls, where osg/series/dver/repo/arch can be found
    "http://mirror.hep.wisc.edu/upstream",
    "https://t2.unl.edu",
    "https://mirror.grid.uchicago.edu/pub",
    "https://linux-mirrors.fnal.gov/linux/"
]

threshold = 24 #hours
timeout = 10 #seconds
socket.setdefaulttimeout(timeout)

log("Using following parameters")
log("tags:"+str(tags))
log("hosts:"+str(mirrorhosts))
log("archs:"+str(archs))
log("threshold:"+str(threshold)+" (hours)")
log("timeout:"+str(timeout)+" (seconds)")
print()

#gethostname() returns actual instance name (like repo2.opensciencegrid.org)
hostname="repo.opensciencegrid.org"
if socket.gethostname() == "repo-itb.opensciencegrid.org":
    hostname="repo-itb.opensciencegrid.org"

def tagsplit(tag):
    if re.match(r'osg-[0-9.]*-[a-z]*-el[0-9]+-[a-z]*', tag):
        series,branch,dver,repo = tag.split('-')[-4:]
        series += "-" + branch
    # matches osg-2X-elY-empty and contrib, but not the equivalent 3.X tags
    elif re.match(r'osg-[0-9][^.][0-9]*-el[0-9]+-(contrib|empty)', tag):
        series,dver,repo = tag.split('-')[-3:]
        series += "-" + repo
        repo = ''
    else:
        series,dver,repo = tag.split('-')[-3:]
    return series,dver,repo

def mkarchurl(host,tag,arch):
    series,dver,repo = tagsplit(tag)
    return '/'.join([host,'osg',series,dver,repo,arch])

def test(hosts,tag,arch):
    # always include repo.opensciencegrid.org in list
    urllist = [mkarchurl('http://'+hostname,tag,arch)]
    for host in hosts:
        url = mkarchurl(host,tag,arch)
        mdurl=url+"/repodata/repomd.xml"
        log("checking: "+mdurl)
        try:
            response = urllib.request.urlopen(mdurl, timeout=10)
            if response.code != 200:
                log("\tbad(non 200) response.code:"+response.code)
            else:
                #make sure the repository is up-to-date
                lastmod_str = response.headers["Last-Modified"]
                lastmodtime = time.strptime(lastmod_str, "%a, %d %b %Y %H:%M:%S %Z") #Thu, 15 Sep 2011 13:34:06 GMT
                age = (time.mktime(time.gmtime()) - time.mktime(lastmodtime))
                if age > 3600 * threshold:
                    log("\ttoo old ("+str(age)+" seconds old) Last-Modified: "+lastmod_str+" .. ignoring")
                else:
                    urllist.append(url)
                    log("\tall good")
        except urllib.error.HTTPError as e:
            #no such repo on this host..
            log("\tURL caught while processing url:"+url+" "+str(e))
        except urllib.error.URLError as e:
            # Error contacting the host. Remove it from the mirrorhosts for this run.
            log("\tExcluding host due to connection error for url:"+url+" "+str(e))
            mirrorhosts.remove(host)
        except Exception as e:
            log("\tException caught while processing url:"+url+" "+str(e))

    return urllist

log("evacuating live dir for osg")

#replace previous mirror
if os.path.exists("/usr/local/mirror/.osg.prev"):
    shutil.rmtree("/usr/local/mirror/.osg.prev")

if os.path.exists("/usr/local/mirror/.osg.new"):
    os.rename("/usr/local/mirror/.osg.new", "/usr/local/mirror/.osg.prev")

#point mirror to previous
if os.path.lexists("/usr/local/mirror/osg"):
    os.unlink("/usr/local/mirror/osg")

os.symlink(".osg.prev", "/usr/local/mirror/osg")

#create new mirror
for tag in tags:
    log("checking for "+tag)
    series,dver,repo = tagsplit(tag)
    tag_archs = list(archs)  # make a copy
    if "23" in series:  # XXX This script will be replaced before OSG 24 anyway
        tag_archs.append("aarch64")
    repopath = '/'.join(["/usr/local/mirror/.osg.new",series,dver,repo])
    os.makedirs(repopath)
    for arch in tag_archs:
        urllist = test(mirrorhosts,tag,arch)
        f = open(repopath + "/" + arch, "w")
        for m in urllist:
            f.write(m+"\n")
        f.close()

# SOFTWARE-4420: temporary upcoming symlink to 3.5-upcoming
os.symlink("3.5-upcoming", "/usr/local/mirror/.osg.new/upcoming")

#point mirror to new
os.unlink("/usr/local/mirror/osg")
os.symlink(".osg.new", "/usr/local/mirror/osg")

log("all done")
