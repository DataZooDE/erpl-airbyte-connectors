#
# Copyright (c) 2024 DataZoo GmbH, all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from .source import SourceRfcReadTable

def run():
    source = SourceRfcReadTable()
    launch(source, sys.argv[1:])
