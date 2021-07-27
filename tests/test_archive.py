import unittest
from tests.logger import Logger
from tests.prepare_test import SetUpTest
from models.api_archive import ArchivePreviewModel


class TestArchive(unittest.TestCase):
    log = Logger(name='test_archive_api.log')
    test = SetUpTest(log)
    project_code = "unittest_archive_dataops_gr"
    container_id = ''
    file_id = []

    @classmethod
    def setUpClass(cls):
        cls.log = cls.test.log
        cls.app = cls.test.app
        file_data = {
            'filename': 'dataops_gr_archive_test_1.zip',
            'namespace': 'greenroom',
            'project_code': cls.project_code,
            'uploader': 'DataopsGRUnittest'
        }
        try:
            cls.container_id = cls.test.create_project(cls.project_code, name="DataopsGRArchiveTest")
            cls.file1 = cls.test.create_file(file_data)
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
            cls.test.delete_project(cls.container_id)

            if cls.file1 and cls.file1["global_entity_id"]:
                payload = {
                    "file_geid": cls.file1["global_entity_id"]
                }
                cls.app.delete("/v1/archive", json=payload)
        except Exception as e:
            cls.log.error("Please manual delete node and entity")
            cls.log.error(e)
            raise e

    def test_01_create_preview(self):
        self.log.info("\n")
        self.log.info("01 test create_preview".center(80, '-'))
        payload = {
            "file_geid": self.file1["global_entity_id"],
            "archive_preview": {'QAZ-1234_ABC-1234_Dicomzip_Prüfung_edited153928o': {'ABC-1234_Dicomzip_Prüfung200140o': {'101_DTI': {'is_dir': True}}}}
        }
        result = self.app.post(f"/v1/archive", json=payload)
        data = result.get_json()
        self.assertEqual(result.status_code, 200)
        self.assertEqual(data["result"], "success")

    def test_02_get_preview(self):
        self.log.info("\n")
        self.log.info("02 test get_preview".center(80, '-'))
        payload = {
            "file_geid": self.file1["global_entity_id"],
        }
        result = self.app.get(f"/v1/archive", query_string=payload)
        data = result.get_json()
        print(data)
        self.assertEqual(result.status_code, 200)
        self.assertEqual(data["result"], {'QAZ-1234_ABC-1234_Dicomzip_Prüfung_edited153928o': {'ABC-1234_Dicomzip_Prüfung200140o': {'101_DTI': {'is_dir': True}}}})

    def test_03_get_preview_missing_geid(self):
        self.log.info("\n")
        self.log.info("03 test get_preview_missing_geid".center(80, '-'))
        payload = {
        }
        result = self.app.get(f"/v1/archive", query_string=payload)
        data = result.get_json()
        self.assertEqual(result.status_code, 400)
        self.assertEqual(data["result"], "file_geid is required")

    def test_04_get_preview_file_not_found(self):
        self.log.info("\n")
        self.log.info("04 test get_preview_file_not_found".center(80, '-'))
        payload = {
            "file_geid": "notfound",
        }
        result = self.app.get(f"/v1/archive", query_string=payload)
        data = result.get_json()
        self.assertEqual(result.status_code, 404)
        self.assertEqual(data["result"], "Archive preview not found")
