#!/usr/bin/env python3

import argparse
import json
import sys
import os
import idseq_dag.util.files
from idseq_dag.engine.pipeline_flow import PipelineFlow

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('dag_json', help='pipeline dag in json file format')
    parser.add_argument('--no-lazy-run', dest='lazy_run', action='store_false')
    parser.set_defaults(lazy_run=True)
    args = parser.parse_args()
    try:
        flow = PipelineFlow(lazy_run=args.lazy_run,
                            dag_json=args.dag_json)
        print("everything is awesome. idseq dag is valid~")
    except:
        parser.print_help()
        raise
    print("start executing the dag")
    flow.start()
