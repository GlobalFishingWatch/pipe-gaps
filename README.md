<h1 align="center" style="border-bottom: none;"> pipe-gaps </h1>

<p align="center">
  <a href="https://codecov.io/gh/GlobalFishingWatch/pipe-gaps">
    <img alt="Coverage" src="https://codecov.io/gh/GlobalFishingWatch/pipe-gaps/branch/develop/graph/badge.svg?token=OO2L9SXVG0">
  </a>
  <a>
    <img alt="Python versions" src="https://img.shields.io/badge/python-3.9%20%7C%203.10%20%7C%203.11%20%7C%203.12-blue">
  </a>
  <a>
    <img alt="Last release" src="https://img.shields.io/github/v/release/GlobalFishingWatch/pipe-gaps">
  </a>
</p>


Time gap detector for **[AIS]** position messages.

**Features**:
* :white_check_mark: Gaps detection core process.
* :white_check_mark: Gaps detection pipeline.
  - :white_check_mark: command-line interface.
  - :white_check_mark: JSON inputs/outputs.
  - :white_check_mark: BigQuery inputs/outputs.
  - :white_check_mark: Apache Beam integration.
  - :white_check_mark: Incremental (daily) processing.
  - :white_check_mark: Full backfill processing.


[AIS]: https://en.wikipedia.org/wiki/Automatic_identification_system
[Apache Beam]: https://beam.apache.org
[Apache Beam Pipeline Options]: https://cloud.google.com/dataflow/docs/reference/pipeline-options#python
[Google Dataflow]: https://cloud.google.com/products/dataflow?hl=en
[Google BigQuery]: https://cloud.google.com/bigquery
[bigquery-emulator]: https://github.com/goccy/bigquery-emulator
[configure a SSH-key for GitHub]: https://docs.github.com/en/authentication/connecting-to-github-with-ssh/adding-a-new-ssh-key-to-your-github-account
[Dataflow runner]: https://beam.apache.org/documentation/runners/dataflow/
[docker official instructions]: https://docs.docker.com/engine/install/
[docker compose plugin]: https://docs.docker.com/compose/install/linux/
[examples]: examples/
[git installed]: https://git-scm.com/downloads
[git workflow documentation]: GIT-WORKFLOW.md
[Makefile]: Makefile
[pip-tools]: https://pip-tools.readthedocs.io/en/stable/
[requirements.txt]: requirements.txt
[requirements/prod.in]: requirements/prod.in
[segment pipeline]: https://github.com/GlobalFishingWatch/pipe-segment
[slowly changing dimension]: https://en.wikipedia.org/wiki/Slowly_changing_dimension
[Semantic Versioning]: https://semver.org

[ais-gaps.py]: pipe_gaps/queries/ais_gaps.py
[ais-messages.py]: pipe_gaps/queries/ais_messages.py
[core.py]: pipe_gaps/pipeline/beam/transforms/core.py
[detect_gaps.py]: pipe_gaps/pipeline/processes/detect_gaps.py
[gap_detector.py]: pipe_gaps/core/gap_detector.py
[pTransform]: https://beam.apache.org/documentation/programming-guide/#applying-transforms

**Table of contents**:
- [Introduction](#introduction)
- [Definition of gap](#definition-of-gap)
- [Some results](#some-results)
- [Usage](#usage)
  * [Installation](#installation)
  * [Gap detection low level process](#gap-detection-low-level-process)
  * [Gap detection pipeline](#gap-detection-pipeline)
    + [BigQuery output schema](#bigquery-output-schema)
    + [BigQuery data persistence pattern](#bigquery-data-persistence-pattern)
    + [Using from CLI](#using-from-cli)
- [Implementation details](#implementation-details)
  * [:warning: Important note on grouping input AIS messages by `ssvid`](#warning-important-note-on-grouping-input-ais-messages-by-ssvid)
  * [Most relevant modules](#most-relevant-modules)
  * [Flow chart](#flow-chart)
- [References](#references)

## Introduction

<div align="justify">

Not all **AIS** messages that are broadcast by vessels
are recorded by receivers for technical reasons,
such as signal interference in crowded waters,
spatial variability of terrestrial reception,
spatial and temporal variability of satellite reception,
and dropped signals as vessels move from terrestrial coverage
to areas of poor satellite reception.
So, as a result,
it’s not uncommon to see gaps in **AIS** data,
for hours or perhaps even days [[1]](#1). 

Other causes of **AIS** gaps are:
* The **AIS** transponder is turned off or otherwise disabled while at sea.
* The **AIS** transponder has a malfunction.
* The ships systems are powered off while the vessel is at anchor or in port.

AIS gaps detection is essential to identify 
possible *intentional disabling events*,
which can obscure illegal activities,
such as unauthorized fishing activity or
unauthorized transshipments [[1]](#1)[[2]](#2).

</div>

## Definition of gap

<div align="justify">

We create an **AIS** **gap** event when the period of time between
consecutive **AIS** positions from a single vessel exceeds a configured `threshold`.
The `start/end` position messages of the gap are called `OFF/ON` messages,
respectively.

When the period of time between **last** known position
and the last time of the current day exceeds the `threshold`,
we create an **open gap** event.
In that case, the gap will not have an `ON` message,
until it is **closed** in the future when new data arrives.

Input position messages are filtered by `good_seg` field
of the segments table in order to remove noise.
This denoising process happens in the [segment pipeline].

</div>

## Some results

These are some results for 2021-2023.

<div align="center">

![alt text](analysis/gaps.svg)

</div>

## Usage

### Installation

We still don't have a package in PYPI.

First, clone the repository.
Then run inside a virtual environment
```shell
make install
```
Or, if you are going to use the dockerized process, build the docker image:
```shell
make build
```

In order to be able to connect to BigQuery, authenticate and configure the project:
```shell
docker compose run gcloud auth application-default login
docker compose run gcloud config set project world-fishing-827
docker compose run gcloud auth application-default set-quota-project world-fishing-827
```

### Gap detection low level process

<div align="justify">

The gap detection core process takes as input a list of **AIS** messages.
Mandatory fields are `["timestamp", "distance_from_shore_m"]`.
At this level, messages with different **SSVIDs** are not distinguished.
The grouping is done in a higher level abstraction.

</div>

> [!NOTE]
> Currently, the algorithm takes about `(1.75 ± 0.01)` seconds to process 10M messages.  
  Tested on a i7-1355U 5.0GHz processor.


```python
import json
from datetime import timedelta, datetime
from pipe_gaps.core import GapDetector

messages = [
    {
        "ssvid": "226013750",
        "msgid": "295fa26f-cee9-1d86-8d28-d5ed96c32835",
        "timestamp": datetime(2024, 1, 1, 0).timestamp(),
        "receiver_type": "terrestrial",
        "lat": 30.5,
        "lon": 50.6,
        "distance_from_shore_m": 1.0
    },
    {
        "ssvid": "226013750",
        "msgid": "295fa26f-cee9-1d86-8d28-d5ed96c32835",
        "timestamp": datetime(2024, 1, 1, 1).timestamp(),
        "receiver_type": "terrestrial",
        "lat": 30.5,
        "lon": 50.6,
        "distance_from_shore_m": 1.0
    }
]

gd = GapDetector(threshold=timedelta(hours=0, minutes=50))
gaps = gd.detect(messages)
print(json.dumps(gaps, indent=4))
```

### Gap detection pipeline

The gaps detection pipeline is described in the following diagram:

```mermaid
flowchart LR;
    subgraph **Main Inputs**
    A[/**AIS** Messages/]
    end
    
    subgraph **Side Inputs**
    C[/Open Gaps/]
    end

    A ==> B[Detect Gaps]
    C ==> B

    B ==> E
    B ==> D
    B ==> F

    subgraph **Outputs**
    E[\New Gaps\]
    D[\New Open gaps\]
    F[\Closed existing open gaps\]
    end
```

<div align="justify">

Inputs, and outputs can be implemented as different kinds of _sources_ and _sinks_.
Currently JSON files and [Google BigQuery] tables are supported.

The pipeline can be "naive" (without parallelization, was useful for development)
or "beam" (allows parallelization through [Apache Beam] & [Google Dataflow]).

</div>

This is an example on how the pipeline can be configured:
```python
from pipe_gaps import pipeline
from pipe_gaps.utils import setup_logger

setup_logger()

pipe_config = {
    "inputs": [
        {
            "kind": "json",
            "input_file": "pipe_gaps/data/sample_messages_lines.json",
            "lines": True
        },
        {
            "kind": "query",
            "query_name": "messages",
            "query_params": {
                "source_messages": "pipe_ais_v3_published.messages",
                "source_segments": "pipe_ais_v3_published.segs_activity",
                "start_date": "2024-01-01",
                "end_date": "2024-01-03",
                "ssvids": [412331104]
            },
            "mock_db_client": False
        }
    ],
    "side_inputs": [
        {
            "kind": "query",
            "query_name": "gaps",
            "query_params": {
                "source_gaps": "scratch_tomas_ttl30d.pipe_ais_gaps",
                "start_date": "2012-01-01"
            },
            "mock_db_client": False
        }
    ],
    "core": {
        "kind": "detect_gaps",
        "groups_key": "ssvid_day",
        "boundaries_key": "ssvid",
        "filter_range": ["2024-01-02", "2024-01-03"],
        "eval_last": true,
        "threshold": 6,
        "show_progress": false,
        "normalize_output": true
    },
    "outputs": [
        {
            "kind": "json",
            "output_prefix": "gaps"
        },
        {
            "kind": "bigquery",
            "table": "scratch_tomas_ttl30d.pipe_ais_gaps",
            "description": "Gaps for AIS position messages.",
            "schema": "gaps",
            "write_disposition": "WRITE_APPEND"
        }
    ],
    "options": {
        "runner": "direct",
        "region": "us-east1",
        "network": "gfw-internal-network",
        "subnetwork": "regions/us-east1/subnetworks/gfw-internal-us-east1"
    }
}

pipe = pipeline.create(pipe_type="beam", **pipe_config)
pipe.run()
```

All configured _main inputs_ are merged before processing,
and the _outputs_ are written in each sink configured.
In this pipeline, _side inputs_ are existing **open gaps** that can be closed while processing.

You can see more configuration examples [here](config/). 


> [!NOTE]
> The key "options" can be used for custom configuration of each pipeline type.
  For example, you can pass any option available in the [Apache Beam Pipeline Options]. 

> [!CAUTION]
> Date ranges are inclusive for the start date and exclusive for the end date.


#### BigQuery output schema

The schema for the output **gap events** table in BigQuery
is defined in [pipe_gaps/pipeline/schemas/ais-gaps.json](/pipe_gaps/pipeline/schemas/ais-gaps.json).

#### BigQuery data persistence pattern

<div align="justify">

When an **open gap** is closed,
a new **gap** event is created.

In the case of BigQuery output,
this means that we are using a persistence pattern
that matches the [slowly changing dimension] type 2 
(always add new rows).
In consequence, the output table can contain two gap events with the same `gap_id`:
the old **open gap** and the current **closed _active_ gap**.
The versioning of gaps is done with a timestamp `gap_version` field with second precision.

To query all _active_ gaps,
you will just need to query the last versions for every `gap_id`.
For example:
```sql
SELECT *
    FROM (
      SELECT
          *,
          MAX(gap_version)
              OVER (PARTITION BY gap_id)
              AS last_version,
      FROM `world-fishing-827.scratch_tomas_ttl30d.pipe_ais_gaps_filter_no_overlapping_and_short`
    )
    WHERE gap_version = last_version
```

</div>

#### Using from CLI

Instead of running from python code,
you can use the provided command-line interface.

```shell
(.venv) $ pipe-gaps
usage: pipe-gaps [-h] [-c ] [--pipe-type ] [--save-stats | --no-save-stats] [--work-dir ] [-v] [--threshold ] [--sort-method ] [--show-progress | --no-show-progress]
                 [--eval-last | --no-eval-last] [--norm | --no-norm]

    Detects time gaps in AIS position messages.
    The definition of a gap is configurable by a time threshold.

options:
  -h, --help                           show this help message and exit
  -c  , --config-file                  JSON file with pipeline configuration (default: None).
  --pipe-type                          Pipeline type: ['naive', 'beam'].
  --save-stats, --no-save-stats        If passed, saves some statistics.
  --work-dir                           Directory to use as working directory.
  -v, --verbose                        Set logger level to DEBUG.

pipeline core process:
  --threshold                          Minimum time difference (hours) to start considering gaps.
  --sort-method                        Sorting algorithm.
  --show-progress, --no-show-progress  If passed, renders a progress bar.
  --eval-last, --no-eval-last          If passed, evaluates last message of each SSVID to create an open gap.
  --norm, --no-norm                    If passed, normalizes the output.

Example: 
    pipe-gaps -c config/sample-from-file-1.json --threshold 1.3
```

> [!NOTE]
> Any option passed to the CLI not explicitly documented will be inside "options" key of the configuration
  (see above). 

## Implementation details

The pipeline is implemented over a (mostly) generic structure that supports:
1. Grouping all _main inputs_ by some composite key with **SSVID**
    and a time interval (**TI**). For example, you can group by **(SSVID, YEAR)**.
2. Grouping _side inputs_ by **SSVID**.
3. Grouping _boundaries_ (first and last **AIS** messages of each group) by **SSVID**.
4. Processing _main inputs_ groups from 1.
5. Processing _boundaries_ from 4 together with _side inputs from_ 2, both grouped by **SSVID**.

Below there is a [diagram](#flow-chart) that describes this work flow.

> [!TIP]
> In the case of the Apache Beam integration with [Dataflow runner],
  the groups can be processed in parallel accross different workers.


### :warning: Important note on grouping input AIS messages by `ssvid` 

<div align="justify">

The gap detection pipeline fetches **AIS** position messages,
filtering them by `good_seg`, and groups them by `ssvid`.
Since **we know** different vessels can broadcast with the same `ssvid`,
this can potentially lead to the situation in which we have a gap
with an `OFF` message from one vessel
and a `ON` message from another vessel (or viceversa).
This can happen because currently,
the gap detection process just orders
all the messages from the same `ssvid` by `timestamp`
and then evaluates each pair of consecuive messages.
In consequence,
it will just pick the last `OFF` (or the first `ON`)
message in the chain when constructing a gap,
and we could have have "inconsistent" gaps
in the sense we described above.
We believe those will be a very small
amount of the total gaps,
and we aim in the future to find a solution to this problem.
One option could be to use something more stable like `vessel_id`
instead of `ssvid`.

</div>

### Most relevant modules

<div align="center">

| Module | Description |
| --- | --- |
| [ais-gaps.py]     | Encapsulates **AIS** gaps query. |
| [ais-messages.py] | Encapsulates **AIS** position messages query. |
| [core.py]         | Defines core [pTransform] that integrates [detect_gaps.py] to Apache Beam. |
| [detect_gaps.py]  | Defines **DetectGaps** class (core processing step of the pipeline). |
| [gap_detector.py] | Defines lower level **GapDetector** class that computes gaps in a list of **AIS** messages. |

</div>

### Flow chart

```mermaid
flowchart TD;
    A[Read Inputs] ==> |**AIS Messages**| B[Group Inputs]
    C[Read Side Inputs] ==> |**Open Gaps**| D[Group Side Inputs]

    subgraph **Core Transform**
    B ==> |**AIS Messages <br/> by SSVID & TI**| E[Process Groups]
    B ==> |**AIS Messages <br/> by SSVID & TI**| F[Process Boundaries]
    D ==> |**Open Gaps  <br/> by SSVID**| F
    E ==> |**Gaps inside groups**| H[Join Outputs]
    F ==> |**Gaps in boundaries <br/> New open gaps <br/> Closed open gaps**| H
    end

    subgraph .
    H ==> K[Write Outputs]
    K ==> L[(BigQuery)]
    end
```


## References
<a id="1">[1]</a> Welch H., Clavelle T., White T. D., Cimino M. A., Van Osdel J., Hochberg T., et al. (2022). Hot spots of unseen fishing vessels. Sci. Adv. 8 (44), eabq2109. doi: 10.1126/sciadv.abq2109

<a id="1">[2]</a> J. H. Ford, B. Bergseth, C. Wilcox, Chasing the fish oil—Do bunker vessels hold the key to fisheries crime networks? Front. Mar. Sci. 5, 267 (2018).