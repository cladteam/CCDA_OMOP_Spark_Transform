# Python Transforms in Foundry

## Overview

This code repo is the primary transformation repo for ingesting CCDA formatted source data and outputting OMOP formatted
 data that is indended to be research-read.

It is based in Python and PySpark, and implemented in Palantir Foundry.

A substantial amount of mapping logic lives in a a separate repo named `CCDA_OMOP_Conversion_Package`. The functional
breakout between these repos is below:

- CCDA OMOP Spark Transform (this repo): Data movement, spark implememntation, and post-processing. 
    `Many transforms import CCDA_OMOP_Conversion_Package`
- CCDA_OMOP_Conversion_Package: Parsing functions for CCDA XML files and configuration that maps CCDA to OMOP

## Jupyter Development

Spark is powerful, but there is an overhead to spin up spark resources. It may be useful to develop more quickly and
iteratively in the Jupyter notebook within Foundry.

## Unit Testing

It is expected that all changes to this repo are performed via a branch and PR process. PRs should pass unit tests with 
basic test data prior to be marked as "ready" for review and potential merging into `master`.

## Project Docs

Project documentation can be found on Google Drive [here](https://drive.google.com/drive/folders/1CVrA7moPrzp2k9w9TIEfv-RtAL2A6WTl).