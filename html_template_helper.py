#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/8/1 10:23
# @Author  : mazhi
# @Site    :
# @File    : identityno_info.py
import os
from jinja2 import Template
from jinja2 import Environment, FileSystemLoader

PATH = os.path.dirname(os.path.abspath(__file__))
TEMPLATE_ENVIRONMENT = Environment(
    autoescape=False,
    loader=FileSystemLoader(os.path.join(PATH, 'templates')),
    trim_blocks=False)


def render_template(template_filename, context):
    return TEMPLATE_ENVIRONMENT.get_template(template_filename).render(context)