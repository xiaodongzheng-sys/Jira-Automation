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

    def test_resolves_component_and_owner_fields_from_system_market_and_component_rules(self):
        row = InputRow(
            row_number=2,
            values={
                "Market Header": "SG",
                "System Header": "AF",
                "Summary Header": "Fraud Appeal",
            },
            ordered_values=("231685", "Fraud Appeal", "SG", "AF"),
        )
        component_rules = [
            {"system": "AF", "market": "SG", "component": "DBP-Anti-fraud"},
            {"system": "Cards", "market": "SG", "component": "Cards Platform"},
        ]
        component_defaults = [
            {
                "component": "DBP-Anti-fraud",
                "assignee": "owner@npt.sg",
                "dev_pic": "dev@npt.sg",
                "qa_pic": "qa@npt.sg",
                "fix_version": "Planning_26Q2",
            }
        ]
        mappings = [
            FieldMapping("Market", "column:Market Header"),
            FieldMapping("System", "column:System Header"),
            FieldMapping("Summary", "column:Summary Header"),
            FieldMapping("Component", f"component_routes:{json.dumps(component_rules)}"),
            FieldMapping(
                "Assignee",
                f'component_defaults:{json.dumps({"field": "assignee", "rules": component_defaults})}',
            ),
            FieldMapping(
                "Dev PIC",
                f'component_defaults:{json.dumps({"field": "dev_pic", "rules": component_defaults})}',
            ),
            FieldMapping(
                "QA PIC",
                f'component_defaults:{json.dumps({"field": "qa_pic", "rules": component_defaults})}',
            ),
            FieldMapping(
                "Fix Version",
                f'component_defaults:{json.dumps({"field": "fix_version", "rules": component_defaults})}',
            ),
        ]

        resolved = resolve_fields(mappings, row)

        self.assertEqual(resolved["Component"], "DBP-Anti-fraud")
        self.assertEqual(resolved["Assignee"], "owner@npt.sg")
        self.assertEqual(resolved["Dev PIC"], "dev@npt.sg")
        self.assertEqual(resolved["QA PIC"], "qa@npt.sg")
        self.assertEqual(resolved["Fix Version"], "Planning_26Q2")

    def test_component_defaults_raise_when_component_has_no_rule(self):
        row = InputRow(
            row_number=2,
            values={"Market Header": "SG", "System Header": "Unknown"},
            ordered_values=("SG", "Unknown"),
        )
        mappings = [
            FieldMapping("Market", "column:Market Header"),
            FieldMapping("System", "column:System Header"),
            FieldMapping(
                "Component",
                f'component_routes:{json.dumps([{"system": "Unknown", "market": "SG", "component": "Shared"}])}',
            ),
            FieldMapping(
                "Assignee",
                f'component_defaults:{json.dumps({"field": "assignee", "rules": []})}',
            ),
        ]

        with self.assertRaises(FieldResolutionError):
            resolve_fields(mappings, row)

    def test_resolves_choices_source_from_web_config(self):
        row = InputRow(row_number=2, values={}, ordered_values=())
        mappings = [FieldMapping("Component", "choices:DBP-Anti-fraud|Anti-fraud|Fraud")]

        resolved = resolve_fields(mappings, row)

        self.assertEqual(resolved["Component"], "DBP-Anti-fraud|Anti-fraud|Fraud")

    def test_skips_optional_prd_links_when_missing(self):
        row = InputRow(
            row_number=2,
            values={"Market Header": "ID", "Summary Header": "Fraud Appeal"},
            ordered_values=("231685", "Fraud Appeal", "ID"),
        )
        mappings = [
            FieldMapping("Market", "column:Market Header"),
            FieldMapping("Summary", "column:Summary Header"),
            FieldMapping("PRD Link/s", "column:Missing PRD Header"),
        ]

        resolved = resolve_fields(mappings, row)

        self.assertEqual(resolved["Market"], "ID")
        self.assertEqual(resolved["Summary"], "Fraud Appeal")
        self.assertNotIn("PRD Link/s", resolved)

    def test_skips_optional_description_when_missing(self):
        row = InputRow(
            row_number=2,
            values={"Summary Header": "Fraud Appeal"},
            ordered_values=("Fraud Appeal",),
        )
        mappings = [
            FieldMapping("Summary", "column:Summary Header"),
            FieldMapping("Description", "column:Missing Description Header"),
        ]

        resolved = resolve_fields(mappings, row)

        self.assertEqual(resolved["Summary"], "Fraud Appeal")
        self.assertNotIn("Description", resolved)


if __name__ == "__main__":
    unittest.main()
