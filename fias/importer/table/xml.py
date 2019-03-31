# coding: utf-8
from __future__ import unicode_literals, absolute_import

import datetime
# from lxml import etree
try:
    from xml.etree.cElementTree import iterparse, ParseError, SyntaxError
except:
    from xml.etree.ElementTree import iterparse, ParseError, SyntaxError


from django.db import models
from fias.fields import UUIDField
from .table import BadTableError, Table, TableIterator, ParentLookupException

_bom_header = b'\xef\xbb\xbf'


class XMLIterator(TableIterator):

    def __init__(self, fd, model):
        super(XMLIterator, self).__init__(fd=fd, model=model)
        self.related_fields = []
        self.uuid_fields = []
        self.date_fields = []
        for field in self.model._meta.get_fields():
            if field.one_to_one or field.many_to_one:
                self.related_fields.append(field.name)
            elif isinstance(field, UUIDField):
                self.uuid_fields.append(field.name)
            elif isinstance(field, models.DateField):
                self.date_fields.append(field.name)

        # self._context = etree.iterparse(self._fd, events='end')
        self._context = iterparse(fd, events='end')

    def format_row(self, row):
        for key, value in row.items():
            key = key.lower()
            if key in self.uuid_fields:
                yield (key, value or None)
            elif key in self.date_fields:
                yield (key, datetime.datetime.strptime(value, "%Y-%m-%d").date())
            elif key in self.related_fields:
                yield ('{0}_id'.format(key), value)
            else:
                yield (key, value)

    def get_next(self):
        event, row = next(self._context)
        item = self.process_row(row)
        row.clear()
        return item


class XMLTable(Table):
    iterator = XMLIterator

    def __init__(self, filename, **kwargs):
        super(XMLTable, self).__init__(filename=filename, **kwargs)

    def rows(self, tablelist):
        if self.deleted:
            return []
        try:
            return self.iterator(self.filename, self.model)
        except (ParseError, SyntaxError) as e:
            raise BadTableError('Error occurred during opening table `{0}`: {1}'.format(self.name, str(e)))
