import unittest
from decimal import Decimal

from clearskies_aws.backends.dynamo_db_condition_parser import DynamoDBConditionParser


class TestDynamoDBConditionParser(unittest.TestCase):
    def setUp(self):
        """Set up the parser for each test."""
        self.parser = DynamoDBConditionParser()

    def test_to_dynamodb_attribute_value_string(self):
        """Test conversion of a simple string."""
        self.assertEqual(self.parser.to_dynamodb_attribute_value("hello"), {"S": "hello"})

    def test_to_dynamodb_attribute_value_int(self):
        """Test conversion of an integer."""
        self.assertEqual(self.parser.to_dynamodb_attribute_value(123), {"N": "123"})

    def test_to_dynamodb_attribute_value_float(self):
        """Test conversion of a float."""
        self.assertEqual(self.parser.to_dynamodb_attribute_value(123.45), {"N": "123.45"})

    def test_to_dynamodb_attribute_value_decimal(self):
        """Test conversion of a Decimal object."""
        self.assertEqual(self.parser.to_dynamodb_attribute_value(Decimal("99.01")), {"N": "99.01"})

    def test_to_dynamodb_attribute_value_bool_true(self):
        """Test conversion of a boolean True."""
        self.assertEqual(self.parser.to_dynamodb_attribute_value(True), {"BOOL": True})

    def test_to_dynamodb_attribute_value_bool_false(self):
        """Test conversion of a boolean False."""
        self.assertEqual(self.parser.to_dynamodb_attribute_value(False), {"BOOL": False})

    def test_to_dynamodb_attribute_value_none(self):
        """Test conversion of None."""
        self.assertEqual(self.parser.to_dynamodb_attribute_value(None), {"NULL": True})

    def test_to_dynamodb_attribute_value_string_true_false_null(self):
        """Test conversion of string representations of boolean, null, and numbers."""
        self.assertEqual(self.parser.to_dynamodb_attribute_value("true"), {"BOOL": True})
        self.assertEqual(self.parser.to_dynamodb_attribute_value("FALSE"), {"BOOL": False})
        self.assertEqual(self.parser.to_dynamodb_attribute_value("NuLl"), {"NULL": True})
        self.assertEqual(self.parser.to_dynamodb_attribute_value("123"), {"N": "123"})
        self.assertEqual(self.parser.to_dynamodb_attribute_value("-45"), {"N": "-45"})
        self.assertEqual(self.parser.to_dynamodb_attribute_value("123.45"), {"N": "123.45"})
        self.assertEqual(self.parser.to_dynamodb_attribute_value("text"), {"S": "text"})

    def test_to_dynamodb_attribute_value_list(self):
        """Test conversion of a list with mixed data types."""
        val = ["a", 1, True, None, Decimal("2.3")]
        expected = {
            "L": [
                {"S": "a"},
                {"N": "1"},
                {"BOOL": True},
                {"NULL": True},
                {"N": "2.3"},
            ]
        }
        self.assertEqual(self.parser.to_dynamodb_attribute_value(val), expected)

    def test_to_dynamodb_attribute_value_map(self):
        """Test conversion of a dictionary (map)."""
        val = {"key_s": "val", "key_n": 100}
        expected = {"M": {"key_s": {"S": "val"}, "key_n": {"N": "100"}}}
        self.assertEqual(self.parser.to_dynamodb_attribute_value(val), expected)

    def test_to_dynamodb_attribute_value_set_string(self):
        """Test conversion of a set of strings."""
        val = {"a", "b", "a"}
        expected = {"SS": sorted(["a", "b"])}
        self.assertEqual(self.parser.to_dynamodb_attribute_value(val), expected)

    def test_to_dynamodb_attribute_value_set_number(self):
        """Test conversion of a set of numbers (int and Decimal)."""
        val = {1, 2, Decimal("3.0")}
        expected = {"NS": sorted(["1", "2", "3.0"])}
        self.assertEqual(self.parser.to_dynamodb_attribute_value(val), expected)

    def test_to_dynamodb_attribute_value_unsupported(self):
        """Test conversion of an unsupported data type raises TypeError."""

        class MyObject:
            pass

        with self.assertRaises(TypeError):
            self.parser.to_dynamodb_attribute_value(MyObject())

    def test_parse_condition_list_simple(self):
        """Test the internal _parse_condition_list helper method."""
        self.assertEqual(self.parser._parse_condition_list("'a', 'b', 'c'"), ["a", "b", "c"])
        self.assertEqual(self.parser._parse_condition_list("1, 2, 3"), ["1", "2", "3"])
        self.assertEqual(self.parser._parse_condition_list("('a', \"b\")"), ["a", "b"])
        self.assertEqual(self.parser._parse_condition_list(""), [])
        self.assertEqual(self.parser._parse_condition_list("  "), [])
        self.assertEqual(self.parser._parse_condition_list(" 'item1' "), ["item1"])

    def test_parse_condition_equals_string(self):
        """Test parsing an equality condition with a string value."""
        result = self.parser.parse_condition("name = 'John Doe'")
        self.assertEqual(result["column"], "name")
        self.assertEqual(result["operator"], "=")
        self.assertEqual(result["values"], [{"S": "John Doe"}])
        self.assertEqual(result["parsed"], '"name" = ?')
        self.assertEqual(result["table"], "")

    def test_parse_condition_equals_number(self):
        """Test parsing an equality condition with a numeric value."""
        result = self.parser.parse_condition("age = 30")
        self.assertEqual(result["column"], "age")
        self.assertEqual(result["operator"], "=")
        self.assertEqual(result["values"], [{"N": "30"}])
        self.assertEqual(result["parsed"], '"age" = ?')

    def test_parse_condition_greater_than(self):
        """Test parsing a greater than condition."""
        result = self.parser.parse_condition("price > 10.5")
        self.assertEqual(result["column"], "price")
        self.assertEqual(result["operator"], ">")
        self.assertEqual(result["values"], [{"N": "10.5"}])
        self.assertEqual(result["parsed"], '"price" > ?')

    def test_parse_condition_table_column(self):
        """Test parsing a condition with a table-prefixed column."""
        result = self.parser.parse_condition("user.id = 'user123'")
        self.assertEqual(result["table"], "user")
        self.assertEqual(result["column"], "id")
        self.assertEqual(result["operator"], "=")
        self.assertEqual(result["values"], [{"S": "user123"}])
        self.assertEqual(result["parsed"], '"user"."id" = ?')

    def test_parse_condition_is_null(self):
        """Test parsing an 'IS NULL' condition (remapped to IS MISSING)."""
        result = self.parser.parse_condition("email is null")
        self.assertEqual(result["column"], "email")
        self.assertEqual(result["operator"], "IS MISSING")
        self.assertEqual(result["values"], [])
        self.assertEqual(result["parsed"], '"email" IS MISSING')

    def test_parse_condition_is_not_null(self):
        """Test parsing an 'IS NOT NULL' condition (remapped to IS NOT MISSING)."""
        result = self.parser.parse_condition("address is not null")
        self.assertEqual(result["column"], "address")
        self.assertEqual(result["operator"], "IS NOT MISSING")
        self.assertEqual(result["values"], [])
        self.assertEqual(result["parsed"], '"address" IS NOT MISSING')

    def test_parse_condition_like_begins_with(self):
        """Test parsing a 'LIKE value%' condition (becomes BEGINS_WITH)."""
        result = self.parser.parse_condition("name LIKE 'Jo%'")
        self.assertEqual(result["column"], "name")
        self.assertEqual(result["operator"], "BEGINS_WITH")
        self.assertEqual(result["values"], [{"S": "Jo"}])
        self.assertEqual(result["parsed"], 'begins_with("name", ?)')

    def test_parse_condition_like_contains(self):
        """Test parsing a 'LIKE %value%' condition (becomes CONTAINS)."""
        result = self.parser.parse_condition("description LIKE '%word%'")
        self.assertEqual(result["column"], "description")
        self.assertEqual(result["operator"], "CONTAINS")
        self.assertEqual(result["values"], [{"S": "word"}])
        self.assertEqual(result["parsed"], 'contains("description", ?)')

    def test_parse_condition_like_exact(self):
        """Test parsing a 'LIKE value' condition (no wildcards, becomes =)."""
        result = self.parser.parse_condition("tag LIKE 'exactmatch'")
        self.assertEqual(result["column"], "tag")
        self.assertEqual(result["operator"], "=")
        self.assertEqual(result["values"], [{"S": "exactmatch"}])
        self.assertEqual(result["parsed"], '"tag" = ?')

    def test_parse_condition_like_ends_with_error(self):
        """Test that 'LIKE %value' (ends with) raises an error."""
        with self.assertRaisesRegex(ValueError, "DynamoDB PartiQL does not directly support 'ends_with'"):
            self.parser.parse_condition("filename LIKE '%doc'")

    def test_parse_condition_in_list_strings(self):
        """Test parsing an 'IN' condition with a list of strings."""
        result = self.parser.parse_condition("status IN ('active', 'pending')")
        self.assertEqual(result["column"], "status")
        self.assertEqual(result["operator"], "IN")
        self.assertEqual(result["values"], [{"S": "active"}, {"S": "pending"}])
        self.assertEqual(result["parsed"], '"status" IN (?, ?)')

    def test_parse_condition_in_list_numbers(self):
        """Test parsing an 'IN' condition with a list of numbers."""
        result = self.parser.parse_condition("id IN (1, 2, 3)")
        self.assertEqual(result["column"], "id")
        self.assertEqual(result["operator"], "IN")
        self.assertEqual(result["values"], [{"N": "1"}, {"N": "2"}, {"N": "3"}])
        self.assertEqual(result["parsed"], '"id" IN (?, ?, ?)')

    def test_parse_condition_in_list_single_value(self):
        """Test parsing an 'IN' condition with a single value in the list."""
        result = self.parser.parse_condition("id IN (1)")
        self.assertEqual(result["column"], "id")
        self.assertEqual(result["operator"], "IN")
        self.assertEqual(result["values"], [{"N": "1"}])
        self.assertEqual(result["parsed"], '"id" IN (?)')

    def test_parse_condition_contains_function(self):
        """Test parsing a 'CONTAINS' function call."""
        result = self.parser.parse_condition("tags CONTAINS 'important'")
        self.assertEqual(result["column"], "tags")
        self.assertEqual(result["operator"], "CONTAINS")
        self.assertEqual(result["values"], [{"S": "important"}])
        self.assertEqual(result["parsed"], 'contains("tags", ?)')

    def test_parse_condition_begins_with_function(self):
        """Test parsing a 'BEGINS_WITH' function call."""
        result = self.parser.parse_condition("sku BEGINS_WITH 'ABC-'")
        self.assertEqual(result["column"], "sku")
        self.assertEqual(result["operator"], "BEGINS_WITH")
        self.assertEqual(result["values"], [{"S": "ABC-"}])
        self.assertEqual(result["parsed"], 'begins_with("sku", ?)')

    def test_parse_condition_quoted_column(self):
        """Test parsing a condition with a double-quoted column name."""
        result = self.parser.parse_condition('"my-column" = "test value"')
        self.assertEqual(result["column"], "my-column")
        self.assertEqual(result["operator"], "=")
        self.assertEqual(result["values"], [{"S": "test value"}])
        self.assertEqual(result["parsed"], '"my-column" = ?')

    def test_parse_condition_no_operator(self):
        """Test that parsing a condition without a valid operator raises an error."""
        with self.assertRaisesRegex(ValueError, "No supported operators found"):
            self.parser.parse_condition("column value")

    def test_parse_condition_is_operator(self):
        """Test parsing an 'IS' condition."""
        result = self.parser.parse_condition("status IS 'active'")
        self.assertEqual(result["column"], "status")
        self.assertEqual(result["operator"], "IS")
        self.assertEqual(result["values"], [{"S": "active"}])
        self.assertEqual(result["parsed"], '"status" IS ?')

    def test_parse_condition_is_not_operator(self):
        """Test parsing an 'IS NOT' condition."""
        result = self.parser.parse_condition("type IS NOT 'internal'")
        self.assertEqual(result["column"], "type")
        self.assertEqual(result["operator"], "IS NOT")
        self.assertEqual(result["values"], [{"S": "internal"}])
        self.assertEqual(result["parsed"], '"type" IS NOT ?')
