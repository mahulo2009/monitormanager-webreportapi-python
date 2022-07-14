import requests
import re

#todo maybe this executor can make request an return values as json or csv.
#change the run to return a result, and then the result can be converted to
#this formats.

class Executor(object):

    def __init__(self, uri):
        self._uri = uri

    def _parse_raw_test(self, text):
        text = text.split('\n')

        text_header = ','.join(text[3].replace("/", ".").split(",")[1:])
        # todo extract all metadata
        text_header = re.sub(r"\([^)]*\)", "", text_header)

        text_body = [','.join(line.split(",")[1:]) for line in text[4:]]
        text_body = '\n'.join(text_body)

        return text_header, text_body

    def run(self):
        print(self._uri)
        r = requests.get(self._uri)
        if r.status_code == 200:
            return r.text
        else:
            # todo check this
            raise requests.exceptions.HTTPError

    def to_csv(self):
        text = self.run()
        header, body = self._parse_raw_test(text)

        return header + '\n' + body
