import pytest
import sys
import os
script_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '../..'))

sys.path.insert(0,root_dir)
from pipeline.extract.bronze_schema import clean_string

class TestCleanString(object):

    def test_on_empty_string(self):
        assert clean_string('') == ''

    def test_on_normal_string(self):
        assert clean_string('hello') == 'hello'

    def test_on_string_with_commas(self):
        assert clean_string('hello, world') == 'hello  world'   

    def test_on_string_with_tabs(self):
        assert clean_string('hello\tworld') == 'hello world'    

    def test_on_string_with_newlines(self):
        assert clean_string('hello\nworld') == 'hello world'
