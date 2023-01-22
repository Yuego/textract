from ..exceptions import UnknownMethod

from .utils import ShellParser


class Parser(ShellParser):
    """Extract text from doc files using antiword.
    """

    def extract(self, filename, method='', **kwargs):
        if not method or method == 'antiword':
            return self.extract_antiword(filename=filename, **kwargs)

        elif method == 'catdoc':
            return self.extract_catdoc(filename, **kwargs)

        else:
            raise UnknownMethod(method)

    def extract_antiword(self, filename, **kwargs):
        stdout, stderr = self.run(['antiword', filename])
        return stdout

    def extract_catdoc(self, filename, **kwargs):
        stdout, stderr = self.run(['catdoc', filename])
        return stdout
