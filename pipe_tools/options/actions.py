import argparse
import json


class ReadFileAction(argparse.Action):
    """
    argparse action that will optionally read the value of an option from a file

    Example usage:

    parser.add_argument(
    '--dest_schema',
    required=True,
    action=ReadFileAction,
    help='bigquery (json) schema to use for writing messages.  Fields not in the schema will be '
         'packed into a single "extra" field.  Pass JSON directly or use @path/to/file to load from a file.')
    """
    def __call__(self, parser, namespace, values, _):
        if (values.startswith('@')):
            with open(values[1:], 'r') as f:
                setattr(namespace, self.dest, f.read())
        else:
            setattr(namespace, self.dest, values)


class LoadFromFileAction (argparse.Action):
    def __call__ (self, parser, namespace, values, option_string = None):
        with values as f:
            parser.parse_args(f.read().split(), namespace)


class ReadJSONAction(argparse.Action):
    def __call__(self, parser, namespace, values, _):

        if (values.startswith('@')):
            with open(values[1:], 'r') as f:
                value = json.load(f)
        else:
            value = json.loads(values)

        setattr(namespace, self.dest, value)
