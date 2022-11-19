# #jak przetestowac tego pipelinea 
import unittest
from src.informationspread.pipelines.data_processing.pipeline import create_pipeline
from kedro.runner import SequentialRunner


class TestPipeline(unittest.TestCase):

    def test_create_pipeline(self):
         pipelie = create_pipeline()
         runner  = SequentialRunner()
         runner.run(pipelie,)

