from transforms.api import Pipeline

from myproject import datasets

import logging
logger = logging.getLogger(__name__)

my_pipeline = Pipeline()
logger.info("CHRIS discover_transforms(datasets)")
my_pipeline.discover_transforms(datasets)
###my_pipeline.add_transforms(datasets.run_test_docs.compute)
#my_pipeline.add_transforms(datasets.dq_person.compute)
logger.info("CHRIS done in pipeline.py")