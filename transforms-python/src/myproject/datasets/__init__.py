# src/myproject/datasets/__init__.py
#
# --- INPUT CONFIG  ---
OMOP_EAV_DICT_STEP_SIZE = 1000  # Batch size for stepping through OMOP_EAV_DICT. Set to None for all-at-once mode
#INPUT_VERSION = "master"
INPUT_VERSION = "master_Mar09"


INPUT_BASE_PATH = "/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/OMOP_spark"
OMOP_EAV_DICT_FULL_PATH = f"{INPUT_BASE_PATH}/omop_eav_dict_{INPUT_VERSION}"
DUMMY_TRIGGER_PATH = f"{INPUT_BASE_PATH}/_dummy_incremental_reset_trigger_{INPUT_VERSION}"
OMOP_EAV_DICT_RECORD_FULL_PATH = f"{INPUT_BASE_PATH}/omop_eav_dict_record_{INPUT_VERSION}"
# Use below to control the backup_omop_eav_dict output.
OMOP_EAV_DICT_BACKUP_FULL_PATH = f"{INPUT_BASE_PATH}/omop_eav_dict_{INPUT_VERSION}_bkp"

# --- OUTPUT CONFIG ---
OUTPUT_PARENT_DIR = "/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData"
OUTPUT_SUBDIR = "OMOP_spark"

OUTPUT_FULL_BASE_PATH = f"{OUTPUT_PARENT_DIR}/{OUTPUT_SUBDIR}"
