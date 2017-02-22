#!/usr/bin/env python
"""Populate sample/demo database for Iotus deployment.



"""
import os
import subprocess
from optparse import OptionParser
verbose = False
quite = True

CREATE_KEYSPACE_TEMPLATE_DEV = "CREATE KEYSPACE %s WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };"

dirpath = (os.path.dirname(__file__) or ".") + "/"

def vprint(s):
    """Verbose print."""
    if verbose: print(s)


def run_command(cmd, input_str=None):
    """Runs command and in case of error prints output and stderr.

    :param cmd:
    :param input_str:
    :return: True if success, False otherwise
    """
    """Runs command and logs output and stderr."""
    vprint("executing: %s" % cmd)
    #return 0
    input_pipe = subprocess.PIPE if input_str else None
    proc = subprocess.Popen(cmd, stdin=input_pipe, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, shell=True)
    (stdoutdata, stderrdata) = proc.communicate(input=input_str)
    success = True
    if proc.returncode != 0:
        print(
            '%s returned code %s:\n%s' %
            (cmd, proc.returncode, stdoutdata + stderrdata))
        if stderrdata:
            success = False
            #print('Error response: %s' % stderrdata)
    else:
        vprint('%s return code %d: %s' % (cmd, proc.returncode, stdoutdata if stdoutdata else "no stdout"))
        if stderrdata:
            success = False
            print('Error response: %s' % stderrdata)

    return success



def do_populate(hostport, keyspace, cqlsh_cmd, scriptfile):
    """

    :param hostport:
    :param keyspace:
    :param scriptfile:
    :return:
    """
    return run_command("%s %s -k %s -f %s" % (cqlsh_cmd, hostport, keyspace, scriptfile))


# default scripts to execute
scripts = [
    # schema
    "dev/iotus-schema.cql",
    #  admin@admin.org and guest@example.org users
    "dev/iotus-populate-user.cql"
    #"dev/iotus-dev-populate_TEMPLATE.cql",
]

def main():
    global verbose
    # -c "~/.ccm/repository/3.9/bin/cqlsh.py --cqlversion=3.4.2"
    # ./populate_db.py -c ~/.ccm/repository/3.9/bin/cqlsh.py -s 192.168.1.78 iotus_simple3 iotus-keyspace-dev_TEMPLATE.cql
    #usage = "usage: %prog [--host host[:port]] keyspace"
    usage = """usage: %prog [options] keyspace [scriptfile]
if scriptfile not given, execute pre-configured scripts:
""" + str(scripts)
    #parser = OptionParser(usage)
    parser = OptionParser(usage)
    parser.add_option("-s", "--server", dest="hostport",
                      default="localhost",
                      help="server host or host:port to connect to")
    parser.add_option("-c", "--cqlsh", dest="cqlsh_cmd",
                      default="cqlsh",
                      help="optional path to cqlsh command")
    parser.add_option("-n", "--no-keyspace-created",
                      action="store_true", dest="no_keyspace_create",
                      default=False,
                      help="first create keyspace", metavar="KEYSPACE")
    parser.add_option("-v", "--verbose",
                      action="store_true", dest="verbose",
                      help="provide verbose output", metavar="VERBOSE")

    parser.add_option("-q", "--quiet",
            action="store_false", dest="verbose",
            help="don't print status messages to stdout", metavar="NOOUTPUT")

    (options, args) = parser.parse_args()
    if len(args) > 2 or len(args) < 1:
        #print(usage)
        parser.error("incorrect number of arguments")

    keyspace = args[0]
    scriptfile = args[1] if len(args) > 1 else None
    verbose = options.verbose
    if options.verbose:
        print "populating %s/%s" % (options.hostport, keyspace)

    rc = True
    if not options.no_keyspace_create:
        script_text = CREATE_KEYSPACE_TEMPLATE_DEV % keyspace
        rc = run_command("%s %s" % (options.cqlsh_cmd, options.hostport), script_text)
        if not rc:
            print("Can't continue since got an error when creating keyspace.")
            return 1
        #do_populate(options.hostport, keyspace, options.cqlsh_cmd, file)
    if not scriptfile:
        for file in scripts:
            rc = do_populate(options.hostport, keyspace, options.cqlsh_cmd, dirpath + file)
            if not rc:
                print("Exiting prematurely: got error from script %s" % file)
                break
    else:
        rc = do_populate(options.hostport, keyspace, options.cqlsh_cmd, scriptfile)
    return 1 if not rc else 0

if __name__ == "__main__":
    main()