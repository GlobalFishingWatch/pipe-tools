from six.moves.urllib.parse import quote as url_quote
import posixpath as pp
import uuid


class GFW_UUID:

    UUID_URL_BASE='//globalfishingwatch.org'
    SOURCE='source'

    def __init__(self, *args):
        self.uuid = self.create_uuid(*args)

    def __str__(self):
        return str(self.uuid)

    # Create a uuid using the given string(s) which are assembled into a url using UUID_URL_BASE
    @classmethod
    def create_uuid(cls, *args):
        args = list(map(url_quote, args))
        return uuid.uuid5(uuid.NAMESPACE_URL, pp.join(cls.UUID_URL_BASE, *args).lower())

