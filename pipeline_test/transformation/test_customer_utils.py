import pytest
import sys
import os
script_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '../..'))

sys.path.insert(0,root_dir)

from pipeline.transformation.customer_utils import fix_customer_name_string, fix_phone_number_string

class TestFixCustomerNameString(object):

    def test_fix_customer_name_string_w_dits_n_space(self):
        assert fix_customer_name_string("Ad.       ..am Hart") == "Adam Hart"
        
    def test_fix_customer_name_string_w_special_chars(self):
        assert fix_customer_name_string("Pete@#$ Takahito") == "Petea Takahito"

    def test_fix_customer_name_string_w_underscores(self):
        assert fix_customer_name_string("Tam&^*ara Willing___)ham") == "Tamara Willingham"

    def test_fix_customer_name_string_w_nums(self):
        assert fix_customer_name_string("   _Mike Vitt 12313orini") == "Mike Vittorini"  

    def test_fix_customer_name_string_w_apostrophe(self):
        assert fix_customer_name_string("Mary O'Rourke") == "Mary O Rourke"

    def test_fix_customer_name_string_w_camel_case(self):
        assert fix_customer_name_string("John Hus<>><>ton") == "John Huston"

    def test_fix_customer_name_string_w_consecutive_upper_case(self):
        assert fix_customer_name_string("B         ecky Martin") == "Becky Martin"

    def test_fix_customer_name_string_w_german_chars(self):
        assert fix_customer_name_string("Anna Häberlin") == "Anna Haeberlin"  


class TestFixPhoneNumberString(object):

    def test_fix_phone_number_string_w_dits_n_space(self):
        assert fix_phone_number_string("123 456 7890") == "1234567890"

    def test_fix_phone_number_string_w_special_chars(self):
        assert fix_phone_number_string("123-456-7890") == "1234567890"

    def test_fix_phone_number_string_w_underscores(self):
        assert fix_phone_number_string("123_456_7890") == "1234567890"

    def test_fix_phone_number_string_w_nums(self):
        assert fix_phone_number_string("1234567890") == "1234567890"

    def test_fix_phone_number_string_w_error_inp(self):
        assert fix_phone_number_string("12345678901234567890") == "1234567890"

    def test_fix_phone_number_string_w_error_inp_str(self):
        assert fix_phone_number_string("Error!") == ""     
                                      