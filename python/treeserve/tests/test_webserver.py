import json
import unittest
import treeserve_web as web

class WebTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        web.create_tree(test_mode=True)

    def setUp(self):
        web.app.config['TESTING'] = True
        self.app = web.app.test_client()
        with web.app.app_context():
            pass

    def tearDown(self):
        pass

    def test_api(self):
        test_path = ""
        test_depth = 0
        self.assertEqual(self.load_response(path=test_path, depth=test_depth),
                         web.tree.format(path=test_path, depth=test_depth))

    def load_response(self, **data):
        response = self.app.get("/api", query_string = data)
        return json.loads(response.get_data(as_text=True))

if __name__ == '__main__':
    unittest.main()
