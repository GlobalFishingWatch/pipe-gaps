import json

from pipe_gaps import queries
from pipe_gaps.bq_client import BigQueryClient


query_params = {
    "source_gaps": "pipe_ais_v3_published.product_events_ais_gaps",
    "start_date": "2020-01-01",
    # "end_date": "2024-01-02",
    "ssvids": [412331104, 477334300],
}

print("Performing query..")
client = BigQueryClient.build()
query = queries.get_query(query_name="gaps", query_params=query_params)
gaps = client.run_query(query)

print("Done.")

gap = next(gaps)
print(json.dumps(gap, indent=4))
