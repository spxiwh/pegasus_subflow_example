#!/usr/bin/env python3
import subprocess
import os
import sys
import logging

from datetime import datetime
from pathlib import Path

from Pegasus.api import *

logging.basicConfig(level=logging.DEBUG)

# --- Work Directory Setup -----------------------------------------------------
RUN_ID = "local-hierarchy-sharedfs-" + datetime.now().strftime("%s")
TOP_DIR = Path.cwd()
WORK_DIR = TOP_DIR / "work"

try:
    Path.mkdir(WORK_DIR)
except FileExistsError:
    pass

# --- Properties ---------------------------------------------------------------
props = Properties()
#props["pegasus.catalog.replica.file"] = "replicas.yml"
props['condor.accounting_group'] = 'ligo.prod.o3.cbc.bbh.pycbcoffline'
props['pegasus.dir.submit.mapper'] = 'Flat'
props['pegasus.dir.staging.mapper'] = 'Flat'
props['pegasus.dir.storage.mapper'] = 'Replica'
props['pegasus.dir.storage.mapper.replica'] = 'File'
props['pegasus.dir.storage.mapper.replica.file'] = 'output.map'
props['pegasus.mode'] = 'development'
#props['pegasus.transfer.bypass.input.staging'] = 'true'
props.write()

# --- Replicas ---------------------------------------------------------------
with open("input.txt", "w") as f:
    f.write("test input file\n")


# This is the output map for all files known to the top-level workflow
with open("output.map", "w") as f:
    f.write("""k1.txt {}/newoutput/k1.txt pool="local"\n""".format(os.getcwd()))
    f.write("""k2.txt {}/newoutput/k2.txt pool="local"\n""".format(os.getcwd()))

rc = ReplicaCatalog()
rc.add_replica(site="local", lfn="input.txt", pfn=Path(__file__).parent.resolve() / "input.txt")
rc.add_replica(site="local", lfn="subwf1.yml", pfn=Path(__file__).parent.resolve() / "subwf1.yml")
rc.add_replica(site="local", lfn="subwf2.yml", pfn=Path(__file__).parent.resolve() / "subwf2.yml")
#rc.write()


# --- Transformations ---------------------------------------------------------------
try:
    pegasus_config = subprocess.run(
        ["pegasus-config", "--bin"], stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
except FileNotFoundError as e:
    print("Unable to find pegasus-config")

assert pegasus_config.returncode == 0

PEGASUS_BIN_DIR = pegasus_config.stdout.decode().strip()

tc = TransformationCatalog()
keg = Transformation(
            "keg",
            site="local",
            pfn=PEGASUS_BIN_DIR + "/pegasus-keg",
            is_stageable=True
        )

ls = Transformation(
            "ls",
            site="condorpool",
            pfn="/bin/ls",
            is_stageable=False
        )

cat = Transformation(
            "cat",
            site="condorpool",
            pfn="/bin/cat",
            is_stageable=False
        )

tc.add_transformations(keg, ls, cat)
tc.write()

# --- SubWorkflow1 ---------------------------------------------------------------

# Now subworkflow1 will also include a job to output k3.txt. Assume this is
# *not known* to the top-level workflow. When generating the sub-workflow we
# would also generate an output.map containing k3.txt (and maybe k1.txt??).
# k3.txt should be staged out by the sub-workflow and the top-level workflow
# remains ignorant that it exists.

with open("output_sw1.map", "w") as f:
    f.write("""k3.txt {}/newoutput/k3.txt pool="local"\n""".format(os.getcwd()))

# I put this in the ReplicaCatalog here, but in general it would be produced by
# the same job that produces subworkflow1's dax file.

rc.add_replica(site="local", lfn="output_sw1.map", pfn=Path(__file__).parent.resolve() / "output_sw1.map")
output_map_sw1 = File("output_sw1.map")

input_file = File("input.txt")
k1_out = File("k1.txt")
k3_out = File("k3.txt")
wf1 = Workflow("subworkflow-1")
k1 = Job(keg)\
        .add_args("-i", input_file, "-o", k1_out, "-T", 5)\
        .add_inputs(input_file)\
        .add_outputs(k1_out)

ls1 = Job(ls)\
        .add_args("-alh")

k3 = Job(keg)\
        .add_args("-i", input_file, "-o", k3_out, "-T", 5)\
        .add_inputs(input_file)\
        .add_outputs(k3_out)


wf1.add_jobs(k1, ls1, k3)
wf1.write("subwf1.yml")

# --- SubWorkflow2 ---------------------------------------------------------------
k2_out = File("k2.txt")
wf2 = Workflow("subworkflow-2")
k2 = Job(keg)\
        .add_args("-i", k1_out, "-o", k2_out, "-T", 5)\
        .add_inputs(k1_out)\
        .add_outputs(k2_out)

wf2.add_jobs(k2)
wf2.write("subwf2.yml")

# Root
root_wf = Workflow("root")

# we write out the replica catalog into the workflow to make sure it gets inherited
# by the sub workflow, or specify the location to it in the propoerties file
root_wf.add_replica_catalog(rc)

j1 = SubWorkflow("subwf1.yml", _id="subwf1")
j1.add_args('-Dpegasus.dir.storage.mapper.replica.file=output_sw1.map')


j1.add_planner_args(verbose=3, output_sites=['local'])\
        .add_inputs(input_file, output_map_sw1)\
        .add_outputs(k1_out)


j2 = SubWorkflow("subwf2.yml", _id="subwf2")\
        .add_planner_args(verbose=3)\
        .add_inputs(k1_out)\
        .add_outputs(k2_out)

# I need to add the sub-workflow output map to the planning job. We currently
# use this formalism to do it, but can happily change. I guess the issue is
# that in some way we need *both* this map file, and the one saying to stage
# intermediate files back to the main workflow, and it seems that currently we
# can only use one of the two.

root_wf.add_jobs(j1, j2)


try:
    root_wf.plan(submit=True)
except PegasusClientError as e:
    print(e.output)
