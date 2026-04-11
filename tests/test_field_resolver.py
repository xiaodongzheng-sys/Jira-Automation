import unittest
import json

from bpmis_jira_tool.errors import FieldResolutionError
from bpmis_jira_tool.field_resolver import resolve_fields
from bpmis_jira_tool.models import FieldMapping, InputRow


class FieldResolverTests(unittest.TestCase):
    def test_resolves_column_literal_and_template_mappings(self):
        row = InputRow(
            row_number=2,
            values={
                "Summary Source": "Login failure",
                "Project Name": "Atlas",
                "Description": "Detailed issue",
            },
            ordered_values=("ignored", "Login failure", "Atlas", "Detailed issue"),
        )
        mappings = [
            FieldMapping("Summary", "column:Summary Source"),
            FieldMapping("Issue Type", "literal:Bug"),
            FieldMapping("Description", "template:Problem for {{Project Name}}: {{Description}}"),
        ]

        resolved = resolve_fields(mappings, row)

        self.assertEqual(
            resolved,
            {
                "Summary": "Login failure",
                "Issue Type": "Bug",
                "Description": "Problem for Atlas: Detailed issue",
            },
        )

    def test_raises_for_missing_value(self):
        row = InputRow(row_number=2, values={}, ordered_values=())
        mappings = [FieldMapping("Summary", "column:Missing")]

        with self.assertRaises(FieldResolutionError):
            resolve_fields(mappings, row)

    def test_resolves_market_choice_mapping_from_web_config(self):
        row = InputRow(
            row_number=2,
            values={"Market Header": "ID"},
            ordered_values=("231685", "Fraud Appeal Journey", "ID", "https://prd"),
        )
        mappings = [
            FieldMapping("Market", "column:Market Header"),
            FieldMapping(
                "Component",
                f'market_choices:{json.dumps({"ID": "DBP-Anti-fraud", "SG": "Fraud", "PH": "", "Regional": ""})}',
            ),
        ]

        resolved = resolve_fields(mappings, row)

        self.assertEqual(resolved["Market"], "ID")
        self.assertEqual(resolved["Component"], "DBP-Anti-fraud")

    def test_resolves_choices_source_from_web_config(self):
        row = InputRow(row_number=2, values={}, ordered_values=())
        mappings = [FieldMapping("Component", "choices:DBP-Anti-fraud|Anti-fraud|Fraud")]

        resolved = resolve_fields(mappings, row)

        self.assertEqual(resolved["Component"], "DBP-Anti-fraud|Anti-fraud|Fraud")


if __name__ == "__main__":
    unittest.main()
