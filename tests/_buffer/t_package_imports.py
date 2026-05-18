import importlib
import unittest


class PackageImportTests(unittest.TestCase):
    def test_domain_and_workflows_import(self) -> None:
        modules = [
            "graphregistry.domain",
            "graphregistry.domain.models",
            "graphregistry.domain.interfaces",
            "graphregistry.workflows",
            "graphregistry.workflows.messages",
            "graphregistry.workflows.operations",
        ]
        for module_name in modules:
            with self.subTest(module=module_name):
                importlib.import_module(module_name)


if __name__ == "__main__":
    unittest.main()
