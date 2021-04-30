import unittest
from tests.logger import Logger
from tests.prepare_test import SetUpTest


class TestProjectFileCheck(unittest.TestCase):
    log = Logger(name='test_count_api.log')
    test = SetUpTest(log)
    project_code = "unittest_count_api"
    container_id = ''
    file_id = []
    file_guid = []

    @classmethod
    def setUpClass(cls):
        cls.log = cls.test.log
        cls.app = cls.test.app
        file_data = {
            'filename': 'dataops_gr_test_1',
            'namespace': 'greenroom',
            'project_code': cls.project_code,
            'file_type': 'raw',
            'uploader': 'DataopsGRUnittest'
        }
        try:
            cls.container_id = cls.test.create_project(cls.project_code)
            cls.file1 = cls.test.create_file(file_data)
            file_data["filename"] = 'dataops_gr_test_2'
            file_data["file_type"] = 'processed'
            file_data["process_pipeline"] = 'dataops_unittest'
            file_data["uploader"] = 'DataopsGRUnittest2'
            cls.file2 = cls.test.create_file(file_data)
            file_data["filename"] = 'dataops_gr_test_3'
            file_data["namespace"] = 'core'
            cls.file3 = cls.test.create_file(file_data)
        except Exception as e:
            print(e)
            cls.log.error(f"Failed set up test due to error: {e}")
            raise unittest.SkipTest(f"Failed setup test {e}")

    @classmethod
    def tearDownClass(cls):
        cls.log.info("\n")
        cls.log.info("START TEAR DOWN PROCESS")
        try:
            cls.test.delete_file_node(cls.file1["id"])
            cls.test.delete_file_entity(cls.file1["guid"])
            cls.test.delete_project(cls.container_id)
        except Exception as e:
            cls.log.error("Please manual delete node and entity")
            cls.log.error(e)
            raise e

    def test_01_get_count(self):
        self.log.info("\n")
        self.log.info("01 test get_count".center(80, '-'))
        result = self.app.get(f"/v2/containers/{self.container_id}/files/count")
        self.assertEqual(result.status_code, 200)
        data = result.get_json()
        self.log.info(f"JSON DATA: {data}")
        self.assertEqual(data["result"]["raw_file_count"], 1)
        self.assertEqual(data["result"]["process_file_count"], 1)

    def test_02_get_count_uploader(self):
        self.log.info("\n")
        self.log.info("02 test get_count_uploader".center(80, '-'))
        params = {
            "uploader": "DataopsGRUnittest"
        }
        result = self.app.get(f"/v2/containers/{self.container_id}/files/count", query_string=params)
        self.assertEqual(result.status_code, 200)
        data = result.get_json()
        self.log.info(f"JSON DATA: {data}")
        self.assertEqual(data["result"]["raw_file_count"], 1)
        self.assertEqual(data["result"]["process_file_count"], 0)


