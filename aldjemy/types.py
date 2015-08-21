# coding: utf-8

import django
from sqlalchemy import types, ForeignKey


def simple(typ):
    return lambda field: typ()


def varchar(field):
    return types.String(length=field.max_length)


def foreign_key(field):
    if django.VERSION < (1, 8):
        parent_model = field.related.parent_model
    elif django.VERSION < (1, 9):
        parent_model = field.related.model
    else:
        parent_model = field.related_model

    target = parent_model._meta
    target_table = target.db_table
    target_pk = target.pk.column
    return types.Integer, ForeignKey('%s.%s' % (target_table, target_pk))

class EnumTypeAdapter(types.TypeDecorator):
    impl = types.SMALLINT

    def __init__(self, enum = None, *args, **kwargs):
        self.enum = enum
        super().__init__(*args, **kwargs)

    def process_bind_param(self, value, dialect):
        if value is None:
            return None
        return int(value)

    def process_result_value(self, value, dialect):
        if value is not None:
            value = self.enum(value)
        return value

