import re
import json


class Response(object):
    """
    A class used to represent a response from the Monitor Manager Web Report Backend
    """
    def __init__(self, r):
        self._r = r

    def _parse_raw_text(self, text: object) -> object:
        """
        Parse the raw text coming from report request to the Monitor Manager Web Report Backend

        :param text:

                The raw format is:

            totalSamples    totalDisplaySamplesByPages  totalPages  page[X-Y]
            <number>        <number>                    <number>    <number>
            <blank line>
            TimeStamp       TimeStampLong   [Monitor1(Unit)]    [Monitor2(Unit)]    ... [MonitorN(Unit)]
            <date1>         <number>       <number>             <number>            ... <number>
            ...
            <date(n)>       <number>       <number>             <number>            ... <number>

        :return:

            Two variable:
                
                - header: The head of the report response:
            
                    TimeStamp       TimeStampLong   [Monitor1(Unit)]    [Monitor2(Unit)]    ... [MonitorN(Unit)]
                
                - body: The body of the report response
            
                    <date1>         <number>       <number>             <number>            ... <number>
                    ...
                    <date(n)>       <number>       <number>             <number>            ... <number>
        """
        text = text.split('\n')

        text_header = ','.join(text[3].replace("/", ".").split(",")[1:])
        # todo extract all metadata
        text_header = re.sub(r"\([^)]*\)", "", text_header)

        text_body = [','.join(line.split(",")[1:]) for line in text[4:]]
        text_body = '\n'.join(text_body)

        return text_header, text_body

    def to_csv(self):
        """
        Convert the raw text coming from report request to the Monitor Manager Web Report Backend to csv format
        """
        header, body = self._parse_raw_text(self._r.text)

        return header + '\n' + body

    def to_json(self):
        """
        Convert the raw text coming from report request to the Monitor Manager Web Report Backend to json format
        """
        return json.loads(self._r.text)
