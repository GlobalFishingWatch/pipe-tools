from apache_beam.io.filesystems import FileSystems


def parse_source_sink(path):
    scheme = FileSystems.get_scheme(path)

    if scheme == 'query':
        # path contains a sql query
        # strip off the scheme and just return the rest
        path = path[8:]
    elif scheme == 'bq':
        # path is a reference to a big query table
        # strip off the scheme and just return the table id in path
        path = path[5:]
    elif scheme == 'gs':
        scheme = 'file'  # file in Google Cloud Storage
    elif scheme is None:
        # could be a local file or a sql query
        if path[0] in ('.', '/'):
            scheme = 'file'  # local file
        else:
            scheme = 'query'

    return scheme, path