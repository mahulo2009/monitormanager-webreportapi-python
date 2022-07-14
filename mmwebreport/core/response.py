import re
import json


class Response(object):

    def __init__(self, r):
        self._r = r

    def _parse_raw_test(self, text):

        text = text.split('\n')

        text_header = ','.join(text[3].replace("/", ".").split(",")[1:])
        # todo extract all metadata
        text_header = re.sub(r"\([^)]*\)", "", text_header)

        text_body = [','.join(line.split(",")[1:]) for line in text[4:]]
        text_body = '\n'.join(text_body)

        return text_header, text_body

    def to_csv(self):

        header, body = self._parse_raw_test(self._r.text)

        return header + '\n' + body

    def to_json(self):

        return json.loads(self._r.text)
