import argparse


class ReadFileAction(argparse.Action):
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