import ujson

import apache_beam as beam
from apache_beam import PTransform
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.io.gcp.bigquery import parse_table_schema_from_json
from apache_beam.io.gcp.internal.clients.bigquery import TableSchema

def parse_table_schema(schema):
    """
    Accepts a BigQuery tableschema as a string, dict (from json), or bigquery.TabelSchema and returns
    a bigquery.TableSchema

    String Format

    "[FIELD]:[DATA_TYPE],[FIELD]:[DATA_TYPE]"

    dict format

    {
      "fields": [
        {
          "name": "[FIELD]",
          "type": "[DATA_TYPE]"
        },
        {
          "name": "[FIELD]",
          "type": "[DATA_TYPE]"
        }
    ]}

    see https://cloud.google.com/bigquery/data-types
    see https://cloud.google.com/bigquery/docs/schemas#specifying_a_schema_file


    """
    if schema is None:
        return schema
    elif isinstance(schema, TableSchema):
        return schema
    elif isinstance(schema, basestring):
        # try to parse json into dict
        try:
            schema = ujson.loads(schema)
        except ValueError, e:
            pass

    if isinstance(schema, basestring):
        # if it is still a string, then it must not be json.  Assume it is string representation
        return WriteToBigQuery.get_table_schema_from_string(schema)
    elif isinstance(schema, dict):
        # either it came in as a dict or it got converted from json earlier
        return parse_table_schema_from_json(ujson.dumps(schema))
    else:
        raise TypeError('Unexpected schema argument: %s.' % schema)


