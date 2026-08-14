import ast
import pathlib
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[1]


class PythonCompatibilityTests(unittest.TestCase):
    def test_path_write_text_does_not_use_newline_keyword(self) -> None:
        for path in sorted((ROOT / "scripts").glob("*.py")):
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            for node in ast.walk(tree):
                if not isinstance(node, ast.Call):
                    continue
                if not isinstance(node.func, ast.Attribute):
                    continue
                if node.func.attr != "write_text":
                    continue
                self.assertNotIn(
                    "newline",
                    {keyword.arg for keyword in node.keywords},
                    f"{path.name}:{node.lineno} uses a Path.write_text "
                    "keyword unavailable on the production Python",
                )


if __name__ == "__main__":
    unittest.main()
